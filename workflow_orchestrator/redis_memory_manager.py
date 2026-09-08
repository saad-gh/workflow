"""
Redis memory management and cleanup for workflow orchestration.

Handles:
- Memory limit monitoring
- Automatic cleanup of expired data
- Eviction policy configuration
- Key TTL management
- Memory usage tracking per namespace
"""
import logging
import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import redis.asyncio as async_redis
from django.conf import settings

logger = logging.getLogger(__name__)


class RedisMemoryManager:
    """
    Manages Redis memory usage for workflow orchestration.

    Features:
    - Memory usage monitoring
    - Automatic cleanup of expired keys
    - TTL enforcement
    - Namespace-based usage tracking
    """

    # Default TTLs (in seconds)
    DEFAULT_TTLS = {
        'task_queue': 86400,           # 24 hours (active queues)
        'result_events': 3600,         # 1 hour (should be processed quickly)
        'run_state': 86400,            # 24 hours
        'task_result': 7200,           # 2 hours (results cached)
        'quota': 86400,                # 24 hours (daily quotas)
        'dedup': 2592000,              # 30 days (monthly dedup)
        'idempotency': 604800,         # 7 days (weekly idempotency)
        'checkpoint': 604800,          # 7 days (workflow checkpoints)
        'webhook_rate_limit': 60,      # 1 minute (rate limiting)
    }

    def __init__(self, redis_client: async_redis.Redis, memory_limit_mb: int = 4096):
        """
        Initialize Redis memory manager.

        Args:
            redis_client: Async Redis client
            memory_limit_mb: Redis memory limit in MB (should match redis.conf maxmemory)
        """
        self.redis = redis_client
        self.memory_limit_mb = memory_limit_mb
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024

    async def get_memory_info(self) -> Dict:
        """
        Get Redis memory usage information.

        Returns:
            {
                'used_memory': 1073741824,  # bytes
                'used_memory_mb': 1024,
                'used_memory_human': '1.00G',
                'used_memory_rss': 1200000000,
                'maxmemory': 4294967296,
                'maxmemory_mb': 4096,
                'maxmemory_human': '4.00G',
                'maxmemory_policy': 'allkeys-lru',
                'mem_fragmentation_ratio': 1.12,
                'evicted_keys': 0,
                'percent_used': 25.0
            }
        """
        info = await self.redis.info('memory')

        used_memory = info.get('used_memory', 0)
        maxmemory = info.get('maxmemory', self.memory_limit_bytes)

        return {
            'used_memory': used_memory,
            'used_memory_mb': used_memory // (1024 * 1024),
            'used_memory_human': info.get('used_memory_human', 'N/A'),
            'used_memory_rss': info.get('used_memory_rss', 0),
            'maxmemory': maxmemory,
            'maxmemory_mb': maxmemory // (1024 * 1024),
            'maxmemory_human': info.get('maxmemory_human', 'N/A'),
            'maxmemory_policy': info.get('maxmemory_policy', 'noeviction'),
            'mem_fragmentation_ratio': info.get('mem_fragmentation_ratio', 1.0),
            'evicted_keys': info.get('evicted_keys', 0),
            'percent_used': (used_memory / maxmemory * 100) if maxmemory > 0 else 0
        }

    async def get_namespace_memory_usage(self) -> Dict[str, int]:
        """
        Estimate memory usage by key namespace (pattern-based).

        Returns:
            {
                'task_queue': 104857600,  # bytes
                'result_events': 52428800,
                'run_state': 10485760,
                'quota': 5242880,
                'dedup': 20971520,
                'other': 1048576
            }
        """
        namespaces = {
            'task_queue': 'task_queue:*',
            'result_events': 'result_events:*',
            'run_state': 'run_state:*',
            'task_result': 'task_result:*',
            'quota': 'quota:*',
            'dedup': 'dedup:*',
            'idempotency': 'idempotency:*',
            'checkpoint': 'checkpoint:*',
            'webhook_rate_limit': 'webhook_rate_limit:*',
        }

        usage = {}
        for name, pattern in namespaces.items():
            total_bytes = 0
            key_count = 0

            async for key in self.redis.scan_iter(match=pattern, count=100):
                try:
                    # Get memory usage for this key
                    mem = await self.redis.memory_usage(key)
                    if mem:
                        total_bytes += mem
                        key_count += 1
                except Exception as e:
                    logger.debug(f"Error getting memory for key {key}: {e}")

            usage[name] = {
                'bytes': total_bytes,
                'mb': total_bytes / (1024 * 1024),
                'keys': key_count
            }

        return usage

    async def cleanup_expired_keys(self) -> Dict[str, int]:
        """
        Cleanup expired keys that don't have TTL set.

        This handles keys that were created without TTL or where TTL expired.
        Looks for keys older than their expected lifetime.

        Returns:
            {
                'task_queue': 5,  # keys deleted
                'result_events': 10,
                'total': 15
            }
        """
        deleted = {}
        total = 0

        # Cleanup old task queues (>24 hours with no activity)
        deleted['task_queue'] = await self._cleanup_old_queues()
        total += deleted['task_queue']

        # Cleanup old result events (>1 hour)
        deleted['result_events'] = await self._cleanup_old_events()
        total += deleted['result_events']

        # Cleanup completed run states (>24 hours)
        deleted['run_state'] = await self._cleanup_completed_runs()
        total += deleted['run_state']

        # Cleanup old task results (>2 hours)
        deleted['task_result'] = await self._cleanup_old_results()
        total += deleted['task_result']

        # Cleanup expired deduplication sets (>30 days)
        deleted['dedup'] = await self._cleanup_old_dedup_sets()
        total += deleted['dedup']

        deleted['total'] = total

        if total > 0:
            logger.info(f"Cleanup: deleted {total} expired keys: {deleted}")

        return deleted

    async def _cleanup_old_queues(self) -> int:
        """Cleanup empty task queues older than 24 hours"""
        deleted = 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        async for key in self.redis.scan_iter(match='task_queue:*', count=100):
            try:
                # Check if queue is empty
                length = await self.redis.llen(key)
                if length > 0:
                    continue

                # Check TTL
                ttl = await self.redis.ttl(key)
                if ttl == -1:  # No TTL set
                    # Delete empty queues without TTL
                    await self.redis.delete(key)
                    deleted += 1
                elif ttl > self.DEFAULT_TTLS['task_queue']:
                    # Reset TTL if too long
                    await self.redis.expire(key, self.DEFAULT_TTLS['task_queue'])

            except Exception as e:
                logger.error(f"Error cleaning queue {key}: {e}")

        return deleted

    async def _cleanup_old_events(self) -> int:
        """Cleanup old result events (should be processed within 1 hour)"""
        deleted = 0

        async for key in self.redis.scan_iter(match='result_events:*', count=100):
            try:
                # Check length
                length = await self.redis.llen(key)

                # If empty, delete
                if length == 0:
                    await self.redis.delete(key)
                    deleted += 1
                else:
                    # Set TTL if not set
                    ttl = await self.redis.ttl(key)
                    if ttl == -1:
                        await self.redis.expire(key, self.DEFAULT_TTLS['result_events'])

            except Exception as e:
                logger.error(f"Error cleaning events {key}: {e}")

        return deleted

    async def _cleanup_completed_runs(self) -> int:
        """Cleanup completed/failed run states older than 24 hours"""
        deleted = 0

        async for key in self.redis.scan_iter(match='run_state:*', count=100):
            try:
                run_state_json = await self.redis.get(key)
                if not run_state_json:
                    continue

                run_state = json.loads(run_state_json)
                status = run_state.get('status')

                # Delete completed/failed runs without TTL
                if status in ('completed', 'failed', 'cancelled'):
                    ttl = await self.redis.ttl(key)
                    if ttl == -1:
                        await self.redis.expire(key, self.DEFAULT_TTLS['run_state'])
                    elif ttl > self.DEFAULT_TTLS['run_state']:
                        await self.redis.expire(key, self.DEFAULT_TTLS['run_state'])

            except Exception as e:
                logger.error(f"Error cleaning run state {key}: {e}")

        return deleted

    async def _cleanup_old_results(self) -> int:
        """Cleanup old task results"""
        deleted = 0

        async for key in self.redis.scan_iter(match='task_result:*', count=100):
            try:
                ttl = await self.redis.ttl(key)
                if ttl == -1:
                    await self.redis.expire(key, self.DEFAULT_TTLS['task_result'])
                elif ttl > self.DEFAULT_TTLS['task_result']:
                    await self.redis.expire(key, self.DEFAULT_TTLS['task_result'])

            except Exception as e:
                logger.error(f"Error cleaning result {key}: {e}")

        return deleted

    async def _cleanup_old_dedup_sets(self) -> int:
        """Cleanup old deduplication sets (>30 days)"""
        deleted = 0

        async for key in self.redis.scan_iter(match='dedup:*', count=100):
            try:
                # Check if set is empty
                size = await self.redis.scard(key)
                if size == 0:
                    await self.redis.delete(key)
                    deleted += 1
                else:
                    # Ensure TTL is set
                    ttl = await self.redis.ttl(key)
                    if ttl == -1:
                        await self.redis.expire(key, self.DEFAULT_TTLS['dedup'])

            except Exception as e:
                logger.error(f"Error cleaning dedup set {key}: {e}")

        return deleted

    async def enforce_ttls(self) -> int:
        """
        Enforce TTLs on keys that should have them.

        Returns:
            Number of keys updated with TTL
        """
        updated = 0

        for namespace, ttl in self.DEFAULT_TTLS.items():
            pattern = f"{namespace}:*"

            async for key in self.redis.scan_iter(match=pattern, count=100):
                try:
                    current_ttl = await self.redis.ttl(key)

                    # If no TTL set (-1) or TTL is too long
                    if current_ttl == -1 or current_ttl > ttl:
                        await self.redis.expire(key, ttl)
                        updated += 1

                except Exception as e:
                    logger.error(f"Error setting TTL for {key}: {e}")

        if updated > 0:
            logger.info(f"Enforced TTLs on {updated} keys")

        return updated

    async def check_memory_health(self) -> Tuple[bool, str, Dict]:
        """
        Check Redis memory health.

        Returns:
            (is_healthy, message, details)
        """
        info = await self.get_memory_info()

        percent_used = info['percent_used']
        evicted_keys = info['evicted_keys']
        fragmentation = info['mem_fragmentation_ratio']

        details = {
            'percent_used': percent_used,
            'evicted_keys': evicted_keys,
            'fragmentation_ratio': fragmentation,
            'used_mb': info['used_memory_mb'],
            'limit_mb': info['maxmemory_mb']
        }

        # Check critical conditions
        if percent_used > 95:
            return False, f"CRITICAL: Redis memory at {percent_used:.1f}%", details

        # Check warning conditions
        if percent_used > 85:
            return True, f"WARNING: Redis memory at {percent_used:.1f}%", details

        # Check eviction
        if evicted_keys > 1000:
            return True, f"WARNING: {evicted_keys} keys evicted", details

        # Check fragmentation
        if fragmentation > 1.5:
            return True, f"WARNING: High fragmentation ({fragmentation:.2f})", details

        return True, f"Redis memory healthy: {percent_used:.1f}% used", details

    async def get_largest_keys(self, limit: int = 10) -> List[Dict]:
        """
        Find largest keys in Redis for debugging.

        Args:
            limit: Number of keys to return

        Returns:
            [
                {'key': 'task_queue:123:abc', 'bytes': 1048576, 'type': 'list'},
                ...
            ]
        """
        keys_with_size = []

        # Sample keys (Redis SCAN is probabilistic)
        async for key in self.redis.scan_iter(count=1000):
            try:
                key_type = await self.redis.type(key)
                memory = await self.redis.memory_usage(key)

                if memory:
                    keys_with_size.append({
                        'key': key.decode('utf-8') if isinstance(key, bytes) else key,
                        'bytes': memory,
                        'mb': memory / (1024 * 1024),
                        'type': key_type
                    })

            except Exception as e:
                logger.debug(f"Error getting size for {key}: {e}")

        # Sort by size
        keys_with_size.sort(key=lambda x: x['bytes'], reverse=True)

        return keys_with_size[:limit]

    async def configure_redis_optimal_settings(self) -> Dict[str, str]:
        """
        Configure Redis for optimal workflow orchestration.

        Sets:
        - maxmemory-policy: allkeys-lru (evict least recently used)
        - maxmemory: configured limit
        - save: disabled (we use Redis as cache, not persistence)

        Returns:
            Configuration applied
        """
        config = {}

        try:
            # Set eviction policy (LRU across all keys)
            await self.redis.config_set('maxmemory-policy', 'allkeys-lru')
            config['maxmemory-policy'] = 'allkeys-lru'

            # Set memory limit
            await self.redis.config_set('maxmemory', str(self.memory_limit_bytes))
            config['maxmemory'] = f"{self.memory_limit_mb}MB"

            # Disable RDB snapshots (we don't need persistence)
            await self.redis.config_set('save', '')
            config['save'] = 'disabled'

            logger.info(f"Redis configured for optimal workflow orchestration: {config}")

        except Exception as e:
            logger.error(f"Error configuring Redis: {e}")

        return config


# Background cleanup task for RQ
def redis_cleanup_task():
    """
    RQ scheduled task: Cleanup expired Redis keys.

    Schedule this to run every 5-10 minutes:
        from django_rq import get_scheduler
        scheduler = get_scheduler('default')
        scheduler.schedule(
            scheduled_time=datetime.utcnow(),
            func=redis_cleanup_task,
            interval=300,  # 5 minutes
            repeat=None
        )
    """
    import redis.asyncio as async_redis
    async def _cleanup():
        # Get Redis connection using Django settings
        redis_client = async_redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_AUTH
        )

        try:
            manager = RedisMemoryManager(redis_client)

            # Cleanup expired keys
            deleted = await manager.cleanup_expired_keys()
            logger.info(f"Redis cleanup: {deleted}")

            # Enforce TTLs
            updated = await manager.enforce_ttls()

            # Check memory health
            is_healthy, message, details = await manager.check_memory_health()
            if not is_healthy:
                logger.error(f"Redis memory health check failed: {message} - {details}")
            elif 'WARNING' in message:
                logger.warning(f"Redis memory warning: {message} - {details}")

            return {
                'deleted': deleted,
                'ttls_updated': updated,
                'memory_health': message,
                'memory_details': details
            }

        finally:
            await redis_client.close()

    import asyncio
    return asyncio.run(_cleanup())
