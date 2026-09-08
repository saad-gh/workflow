"""
Container lifecycle management for production deployments.

Handles:
- Memory limit monitoring and graceful degradation
- Graceful shutdown on SIGTERM/SIGINT
- Workflow checkpoint saving before shutdown
- Health checks and readiness probes
- Resource cleanup
"""
import signal
import psutil
import logging
import asyncio
import os
from typing import Optional, Callable
from datetime import datetime, timezone
from django.conf import settings

logger = logging.getLogger(__name__)


class ContainerLifecycleManager:
    """
    Manages container lifecycle for Cloud Run and Kubernetes deployments.

    Features:
    - Memory monitoring with configurable thresholds
    - Graceful shutdown handling
    - Checkpoint saving before termination
    - Health/readiness checks
    """

    def __init__(
        self,
        memory_limit_mb: Optional[int] = None,
        memory_warning_threshold: float = 0.80,
        memory_critical_threshold: float = 0.90,
        shutdown_timeout_seconds: int = 30
    ):
        """
        Initialize lifecycle manager.

        Args:
            memory_limit_mb: Container memory limit in MB (auto-detect from env if None)
            memory_warning_threshold: Warn at this percentage of limit (0.80 = 80%)
            memory_critical_threshold: Take action at this percentage (0.90 = 90%)
            shutdown_timeout_seconds: Max time for graceful shutdown
        """
        self.memory_limit_mb = memory_limit_mb or self._detect_memory_limit()
        self.memory_warning_threshold = memory_warning_threshold
        self.memory_critical_threshold = memory_critical_threshold
        self.shutdown_timeout = shutdown_timeout_seconds

        self.is_shutting_down = False
        self.shutdown_handlers = []
        self.checkpoint_handlers = []

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigint)

        logger.info(
            f"Container lifecycle manager initialized: "
            f"memory_limit={self.memory_limit_mb}MB, "
            f"warn={self.memory_warning_threshold:.0%}, "
            f"critical={self.memory_critical_threshold:.0%}"
        )

    def _detect_memory_limit(self) -> int:
        """
        Auto-detect container memory limit from environment.

        Checks:
        1. Cloud Run: CLOUD_RUN_MEMORY env var (e.g., '2Gi')
        2. Kubernetes: /sys/fs/cgroup/memory/memory.limit_in_bytes
        3. Fallback: Total system memory

        Returns:
            Memory limit in MB
        """
        # Check Cloud Run env var from Django settings
        cloud_run_memory = settings.CLOUD_RUN_MEMORY
        if cloud_run_memory:
            # Parse formats like '2Gi', '512Mi'
            import re
            match = re.match(r'(\d+)(Gi|Mi)', cloud_run_memory)
            if match:
                value, unit = match.groups()
                if unit == 'Gi':
                    return int(value) * 1024
                elif unit == 'Mi':
                    return int(value)

        # Check cgroup memory limit (Kubernetes, Docker)
        try:
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                limit_bytes = int(f.read().strip())
                # If limit is very large (>100GB), it's likely unlimited
                if limit_bytes < 100 * 1024 * 1024 * 1024:
                    return limit_bytes // (1024 * 1024)
        except (FileNotFoundError, ValueError):
            pass

        # Fallback: use 80% of total system memory
        total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
        return int(total_memory_mb * 0.8)

    def get_memory_usage(self) -> dict:
        """
        Get current memory usage statistics.

        Returns:
            {
                'used_mb': 1024,
                'limit_mb': 2048,
                'percent': 50.0,
                'available_mb': 1024,
                'warning': False,
                'critical': False
            }
        """
        process = psutil.Process()
        memory_info = process.memory_info()
        used_mb = memory_info.rss // (1024 * 1024)
        percent = (used_mb / self.memory_limit_mb) * 100 if self.memory_limit_mb else 0

        return {
            'used_mb': used_mb,
            'limit_mb': self.memory_limit_mb,
            'percent': percent,
            'available_mb': self.memory_limit_mb - used_mb,
            'warning': percent >= (self.memory_warning_threshold * 100),
            'critical': percent >= (self.memory_critical_threshold * 100)
        }

    def check_memory_health(self) -> tuple[bool, str]:
        """
        Check if memory usage is healthy.

        Returns:
            (is_healthy, message)
        """
        usage = self.get_memory_usage()

        if usage['critical']:
            return False, (
                f"CRITICAL: Memory at {usage['percent']:.1f}% "
                f"({usage['used_mb']}MB / {usage['limit_mb']}MB)"
            )
        elif usage['warning']:
            logger.warning(
                f"Memory warning: {usage['percent']:.1f}% used "
                f"({usage['used_mb']}MB / {usage['limit_mb']}MB)"
            )
            return True, f"Warning: Memory at {usage['percent']:.1f}%"
        else:
            return True, f"Memory healthy: {usage['percent']:.1f}%"

    def should_accept_new_work(self) -> bool:
        """
        Determine if container should accept new work.

        Returns False if:
        - Shutting down
        - Memory usage is critical

        Use this in RQ worker loops and API endpoints.
        """
        if self.is_shutting_down:
            logger.info("Rejecting new work: container is shutting down")
            return False

        usage = self.get_memory_usage()
        if usage['critical']:
            logger.warning(
                f"Rejecting new work: memory critical at {usage['percent']:.1f}%"
            )
            return False

        return True

    def register_shutdown_handler(self, handler: Callable):
        """
        Register a function to be called during graceful shutdown.

        Handler signature: def handler() -> None

        Example:
            def cleanup():
                db_connection.close()
                redis_client.close()

            manager.register_shutdown_handler(cleanup)
        """
        self.shutdown_handlers.append(handler)
        logger.info(f"Registered shutdown handler: {handler.__name__}")

    def register_checkpoint_handler(self, handler: Callable):
        """
        Register a function to save checkpoint state before shutdown.

        Handler signature: async def handler() -> None

        Example:
            async def save_workflow_state():
                # Save current workflow progress to Redis
                await redis.set('checkpoint:run_123', json.dumps(state))

            manager.register_checkpoint_handler(save_workflow_state)
        """
        self.checkpoint_handlers.append(handler)
        logger.info(f"Registered checkpoint handler: {handler.__name__}")

    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM (graceful shutdown request from Cloud Run)"""
        logger.warning("Received SIGTERM - starting graceful shutdown")
        asyncio.create_task(self.graceful_shutdown())

    def _handle_sigint(self, signum, frame):
        """Handle SIGINT (Ctrl+C during development)"""
        logger.warning("Received SIGINT - starting graceful shutdown")
        asyncio.create_task(self.graceful_shutdown())

    async def graceful_shutdown(self):
        """
        Perform graceful shutdown.

        Steps:
        1. Set shutting_down flag (rejects new work)
        2. Save checkpoints (workflow state, queue positions)
        3. Execute shutdown handlers (close connections)
        4. Wait for in-flight work (up to timeout)
        5. Exit
        """
        if self.is_shutting_down:
            logger.info("Shutdown already in progress, ignoring duplicate signal")
            return

        self.is_shutting_down = True
        start_time = datetime.now(timezone.utc)

        logger.warning(
            f"=== GRACEFUL SHUTDOWN STARTED === "
            f"timeout={self.shutdown_timeout}s"
        )

        try:
            # Step 1: Save checkpoints (workflow state)
            if self.checkpoint_handlers:
                logger.info(f"Saving {len(self.checkpoint_handlers)} checkpoints...")
                for handler in self.checkpoint_handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler()
                        else:
                            handler()
                        logger.info(f"Checkpoint saved: {handler.__name__}")
                    except Exception as e:
                        logger.error(
                            f"Error saving checkpoint {handler.__name__}: {e}",
                            exc_info=True
                        )

            # Step 2: Execute shutdown handlers (close connections)
            if self.shutdown_handlers:
                logger.info(f"Running {len(self.shutdown_handlers)} shutdown handlers...")
                for handler in self.shutdown_handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler()
                        else:
                            handler()
                        logger.info(f"Shutdown handler completed: {handler.__name__}")
                    except Exception as e:
                        logger.error(
                            f"Error in shutdown handler {handler.__name__}: {e}",
                            exc_info=True
                        )

            # Step 3: Wait for remaining async tasks (up to timeout)
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            remaining_time = max(0, self.shutdown_timeout - elapsed)

            if remaining_time > 0:
                logger.info(f"Waiting up to {remaining_time:.1f}s for in-flight work...")
                await asyncio.sleep(min(remaining_time, 5))  # Wait max 5s for cleanup

            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.warning(
                f"=== GRACEFUL SHUTDOWN COMPLETE === "
                f"duration={elapsed:.2f}s"
            )

        except Exception as e:
            logger.error(f"Error during graceful shutdown: {e}", exc_info=True)

        finally:
            # Exit process
            import sys
            sys.exit(0)

    def health_check(self, redis_health: Optional[dict] = None) -> dict:
        """
        Health check endpoint for Kubernetes/Cloud Run probes.

        Args:
            redis_health: Optional Redis health check results from RedisMemoryManager

        Returns:
            {
                'status': 'healthy' | 'degraded' | 'unhealthy',
                'memory': {...},
                'redis': {...},  # If redis_health provided
                'shutting_down': bool,
                'timestamp': '...'
            }
        """
        memory = self.get_memory_usage()

        # Determine status based on container memory
        if self.is_shutting_down:
            status = 'unhealthy'
        elif memory['critical']:
            status = 'unhealthy'
        elif memory['warning']:
            status = 'degraded'
        else:
            status = 'healthy'

        # Check Redis health if provided
        if redis_health:
            redis_status = redis_health.get('status', 'unknown')
            if redis_status == 'critical':
                status = 'unhealthy'
            elif redis_status == 'warning' and status == 'healthy':
                status = 'degraded'

        health = {
            'status': status,
            'memory': memory,
            'shutting_down': self.is_shutting_down,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        # Include Redis health if provided
        if redis_health:
            health['redis'] = redis_health

        return health


# Global instance
_lifecycle_manager: Optional[ContainerLifecycleManager] = None


def get_lifecycle_manager() -> ContainerLifecycleManager:
    """
    Get or create global lifecycle manager instance.

    Usage:
        from workflow_orchestrator.container_lifecycle import get_lifecycle_manager

        manager = get_lifecycle_manager()
        if not manager.should_accept_new_work():
            return JsonResponse({'error': 'Service unavailable'}, status=503)
    """
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = ContainerLifecycleManager()
    return _lifecycle_manager


def init_lifecycle_manager(**kwargs):
    """
    Initialize global lifecycle manager with custom config.

    Call this in Django settings or manage.py before starting workers.

    Example:
        init_lifecycle_manager(
            memory_limit_mb=2048,
            memory_warning_threshold=0.80,
            shutdown_timeout_seconds=30
        )
    """
    global _lifecycle_manager
    _lifecycle_manager = ContainerLifecycleManager(**kwargs)
    return _lifecycle_manager
