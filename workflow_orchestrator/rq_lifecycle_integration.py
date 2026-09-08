"""
RQ Worker lifecycle integration with container management.

Integrates ContainerLifecycleManager with RQ workers for:
- Graceful shutdown of workers
- Checkpoint saving for in-progress workflows
- Memory monitoring during task execution
"""
import logging
import asyncio
import json
from typing import Optional
from rq import Worker
from rq.job import Job

logger = logging.getLogger(__name__)


def create_checkpoint_handler(redis_client):
    """
    Create checkpoint handler for saving workflow state before shutdown.

    This handler:
    1. Finds all in-progress workflow runs
    2. Saves checkpoint state to Redis
    3. Marks runs as 'paused' for resume on restart

    Args:
        redis_client: Redis async client

    Returns:
        Async checkpoint handler function
    """
    async def save_workflow_checkpoints():
        """Save all in-progress workflow checkpoints"""
        try:
            logger.info("Saving workflow checkpoints before shutdown...")

            # Find all active run states
            pattern = "run_state:*:*"
            keys = []
            async for key in redis_client.scan_iter(match=pattern):
                keys.append(key)

            saved_count = 0
            for key in keys:
                try:
                    # Get run state
                    run_state_json = await redis_client.get(key)
                    if not run_state_json:
                        continue

                    run_state = json.loads(run_state_json)

                    # Only checkpoint running workflows
                    if run_state.get('status') != 'running':
                        continue

                    # Extract org_id and run_id from key (format: run_state:org_id:run_id)
                    parts = key.decode('utf-8').split(':')
                    if len(parts) < 3:
                        continue

                    org_id = parts[1]
                    run_id = parts[2]
                    queue_key = f"task_queue:{org_id}:{run_id}"

                    # Get remaining tasks in queue
                    queue_length = await redis_client.llen(queue_key)

                    # Update run state to paused
                    run_state['status'] = 'paused'
                    run_state['paused_reason'] = 'container_shutdown'
                    run_state['remaining_tasks'] = queue_length
                    run_state['checkpoint_created_at'] = asyncio.get_event_loop().time()

                    await redis_client.set(
                        key,
                        json.dumps(run_state),
                        ex=86400  # Keep for 24 hours
                    )

                    saved_count += 1
                    logger.info(
                        f"Checkpointed run {run_id} (org {org_id}): "
                        f"{queue_length} tasks remaining"
                    )

                except Exception as e:
                    logger.error(f"Error checkpointing run {key}: {e}", exc_info=True)

            logger.info(f"Saved {saved_count} workflow checkpoints")

        except Exception as e:
            logger.error(f"Error saving checkpoints: {e}", exc_info=True)

    return save_workflow_checkpoints


def create_redis_cleanup_handler(redis_client):
    """
    Create handler to close Redis connection on shutdown.

    Args:
        redis_client: Redis async client

    Returns:
        Async cleanup handler
    """
    async def close_redis():
        """Close Redis connection gracefully"""
        try:
            logger.info("Closing Redis connection...")
            await redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {e}", exc_info=True)

    return close_redis


class LifecycleAwareWorker(Worker):
    """
    RQ Worker with lifecycle management integration.

    Features:
    - Memory monitoring before accepting jobs
    - Graceful shutdown handling
    - Checkpoint saving for in-progress workflows
    """

    def __init__(self, *args, lifecycle_manager=None, redis_async_client=None, **kwargs):
        """
        Initialize lifecycle-aware worker.

        Args:
            lifecycle_manager: ContainerLifecycleManager instance
            redis_async_client: Async Redis client for checkpointing
            *args, **kwargs: Passed to RQ Worker
        """
        super().__init__(*args, **kwargs)
        self.lifecycle_manager = lifecycle_manager
        self.redis_async_client = redis_async_client

        if self.lifecycle_manager and self.redis_async_client:
            # Register checkpoint handler
            checkpoint_handler = create_checkpoint_handler(self.redis_async_client)
            self.lifecycle_manager.register_checkpoint_handler(checkpoint_handler)

            # Register Redis cleanup handler
            cleanup_handler = create_redis_cleanup_handler(self.redis_async_client)
            self.lifecycle_manager.register_shutdown_handler(cleanup_handler)

            logger.info("Worker lifecycle integration initialized")

    def dequeue_job_and_maintain_ttl(self, timeout: Optional[int]) -> Optional[Job]:
        """
        Override to check lifecycle before dequeuing jobs.

        Rejects jobs if:
        - Container is shutting down
        - Memory usage is critical
        """
        if self.lifecycle_manager:
            # Check if we should accept new work
            if not self.lifecycle_manager.should_accept_new_work():
                logger.info("Worker rejecting new jobs (shutting down or memory critical)")
                return None

            # Check memory health
            is_healthy, message = self.lifecycle_manager.check_memory_health()
            if not is_healthy:
                logger.warning(f"Worker health check failed: {message}")
                # Don't return None here - allow current job to finish
                # but log the warning

        return super().dequeue_job_and_maintain_ttl(timeout)

    def perform_job(self, job: Job, queue):
        """
        Override to check memory before executing job.
        """
        if self.lifecycle_manager:
            usage = self.lifecycle_manager.get_memory_usage()
            logger.info(
                f"Starting job {job.id}: "
                f"memory at {usage['percent']:.1f}% "
                f"({usage['used_mb']}MB / {usage['limit_mb']}MB)"
            )

        result = super().perform_job(job, queue)

        if self.lifecycle_manager:
            usage = self.lifecycle_manager.get_memory_usage()
            logger.info(
                f"Completed job {job.id}: "
                f"memory at {usage['percent']:.1f}%"
            )

        return result


def setup_worker_lifecycle(worker: Worker, redis_async_client=None):
    """
    Setup lifecycle management for an existing RQ worker.

    Usage:
        from django_rq import get_worker
        from workflow_orchestrator.container_lifecycle import get_lifecycle_manager
        from workflow_orchestrator.rq_lifecycle_integration import setup_worker_lifecycle

        worker = get_worker('default')
        lifecycle_manager = get_lifecycle_manager()
        setup_worker_lifecycle(worker, redis_async_client)

    Args:
        worker: RQ Worker instance
        redis_async_client: Async Redis client for checkpointing
    """
    from workflow_orchestrator.container_lifecycle import get_lifecycle_manager

    lifecycle_manager = get_lifecycle_manager()

    if redis_async_client:
        # Register checkpoint handler
        checkpoint_handler = create_checkpoint_handler(redis_async_client)
        lifecycle_manager.register_checkpoint_handler(checkpoint_handler)

        # Register Redis cleanup handler
        cleanup_handler = create_redis_cleanup_handler(redis_async_client)
        lifecycle_manager.register_shutdown_handler(cleanup_handler)

    logger.info("Worker lifecycle setup complete")
    return lifecycle_manager


# Example usage in custom management command
"""
from django.core.management.base import BaseCommand
from django_rq import get_worker
import redis.asyncio as async_redis
from workflow_orchestrator.container_lifecycle import init_lifecycle_manager
from workflow_orchestrator.rq_lifecycle_integration import LifecycleAwareWorker

class Command(BaseCommand):
    help = 'Run RQ worker with lifecycle management'

    def handle(self, *args, **options):
        # Initialize lifecycle manager
        lifecycle_manager = init_lifecycle_manager(
            memory_limit_mb=2048,
            memory_warning_threshold=0.80,
            shutdown_timeout_seconds=30
        )

        # Create async Redis client for checkpointing
        redis_client = async_redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_AUTH
        )

        # Create lifecycle-aware worker
        worker = LifecycleAwareWorker(
            ['default'],
            lifecycle_manager=lifecycle_manager,
            redis_async_client=redis_client
        )

        # Start worker
        worker.work()
"""
