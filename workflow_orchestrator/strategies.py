"""
Deduplication and Idempotency strategies for workflow execution.
"""
import hashlib
import json
import logging
from typing import List, Dict, Any, Set, Optional
import redis.asyncio as async_redis

logger = logging.getLogger(__name__)


# ============================================
# Deduplication Strategies
# ============================================

class DeduplicationStrategy:
    """Base class for deduplication strategies"""

    def __init__(self, redis_client: async_redis.Redis, config: dict):
        self.redis = redis_client
        self.config = config

    async def is_duplicate(self, item: dict, dedup_key: str) -> bool:
        """Check if item is a duplicate"""
        raise NotImplementedError

    async def mark_as_seen(self, item: dict, dedup_key: str, ttl: int = 86400):
        """Mark item as seen (store in Redis)"""
        raise NotImplementedError

    async def filter_duplicates(self, items: List[dict], dedup_key: str) -> List[dict]:
        """Filter out duplicate items from a list"""
        unique_items = []
        for item in items:
            if not await self.is_duplicate(item, dedup_key):
                unique_items.append(item)
                await self.mark_as_seen(item, dedup_key)
        return unique_items


class DedupById(DeduplicationStrategy):
    """Deduplicate by single ID field"""

    async def is_duplicate(self, item: dict, dedup_key: str) -> bool:
        id_field = self.config.get('key_fields', ['id'])[0]
        item_id = item.get(id_field)

        if not item_id:
            return False

        redis_key = f"{dedup_key}:{item_id}"
        exists = await self.redis.exists(redis_key)
        return bool(exists)

    async def mark_as_seen(self, item: dict, dedup_key: str, ttl: int = 86400):
        id_field = self.config.get('key_fields', ['id'])[0]
        item_id = item.get(id_field)

        if item_id:
            redis_key = f"{dedup_key}:{item_id}"
            await self.redis.setex(redis_key, ttl, "1")


class DedupByCompositeKey(DeduplicationStrategy):
    """Deduplicate by composite key (multiple fields)"""

    async def is_duplicate(self, item: dict, dedup_key: str) -> bool:
        composite_fields = self.config.get('composite_fields', [])
        if not composite_fields:
            return False

        # Build composite key from multiple fields
        key_parts = [str(item.get(field, '')) for field in composite_fields]
        composite_id = ':'.join(key_parts)

        redis_key = f"{dedup_key}:{composite_id}"
        exists = await self.redis.exists(redis_key)
        return bool(exists)

    async def mark_as_seen(self, item: dict, dedup_key: str, ttl: int = 86400):
        composite_fields = self.config.get('composite_fields', [])
        if not composite_fields:
            return

        key_parts = [str(item.get(field, '')) for field in composite_fields]
        composite_id = ':'.join(key_parts)

        redis_key = f"{dedup_key}:{composite_id}"
        await self.redis.setex(redis_key, ttl, "1")


class DedupByHash(DeduplicationStrategy):
    """Deduplicate by content hash"""

    async def is_duplicate(self, item: dict, dedup_key: str) -> bool:
        content_hash = self._compute_hash(item)
        redis_key = f"{dedup_key}:{content_hash}"
        exists = await self.redis.exists(redis_key)
        return bool(exists)

    async def mark_as_seen(self, item: dict, dedup_key: str, ttl: int = 86400):
        content_hash = self._compute_hash(item)
        redis_key = f"{dedup_key}:{content_hash}"
        await self.redis.setex(redis_key, ttl, "1")

    def _compute_hash(self, item: dict) -> str:
        """Compute SHA256 hash of item content"""
        # Sort keys for consistent hashing
        content = json.dumps(item, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


def get_deduplication_strategy(
    strategy_name: str,
    redis_client: async_redis.Redis,
    config: dict
) -> Optional[DeduplicationStrategy]:
    """Factory function to get deduplication strategy"""
    strategies = {
        'by_id': DedupById,
        'by_composite_key': DedupByCompositeKey,
        'by_hash': DedupByHash,
    }

    strategy_class = strategies.get(strategy_name)
    if not strategy_class:
        logger.warning(f"Unknown deduplication strategy: {strategy_name}")
        return None

    return strategy_class(redis_client, config)


# ============================================
# Idempotency Strategies
# ============================================

class IdempotencyStrategy:
    """Base class for idempotency strategies"""

    def __init__(self, redis_client: async_redis.Redis, config: dict):
        self.redis = redis_client
        self.config = config

    async def check_idempotency(self, operation_id: str) -> Optional[dict]:
        """Check if operation was already executed, return cached result if so"""
        raise NotImplementedError

    async def store_result(self, operation_id: str, result: dict, ttl: int = 86400):
        """Store operation result for idempotency"""
        raise NotImplementedError


class IdempotencyByKey(IdempotencyStrategy):
    """Idempotency using operation keys"""

    async def check_idempotency(self, operation_id: str) -> Optional[dict]:
        """Check if operation already executed"""
        redis_key = f"idempotency:{operation_id}"
        result = await self.redis.get(redis_key)

        if result:
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return None

        return None

    async def store_result(self, operation_id: str, result: dict, ttl: int = 86400):
        """Store operation result"""
        redis_key = f"idempotency:{operation_id}"
        await self.redis.setex(redis_key, ttl, json.dumps(result))


def build_operation_id(task_params: dict, template: str = None) -> str:
    """
    Build operation ID for idempotency checking.

    Args:
        task_params: Task parameters dict
        template: Template for operation ID (e.g., "task_{org_id}_{task_id}_{params_hash}")

    Returns:
        Operation ID string
    """
    if not template:
        # Default: hash of all params
        params_str = json.dumps(task_params, sort_keys=True)
        params_hash = hashlib.sha256(params_str.encode()).hexdigest()[:16]
        return f"op_{params_hash}"

    # TODO: Implement template rendering for operation ID
    return template


def get_idempotency_strategy(
    strategy_name: str,
    redis_client: async_redis.Redis,
    config: dict
) -> Optional[IdempotencyStrategy]:
    """Factory function to get idempotency strategy"""
    strategies = {
        'idempotency_key': IdempotencyByKey,
    }

    strategy_class = strategies.get(strategy_name)
    if not strategy_class:
        logger.warning(f"Unknown idempotency strategy: {strategy_name}")
        return None

    return strategy_class(redis_client, config)


# ============================================
# Utility Functions
# ============================================

async def deduplicate_response_items(
    items: List[dict],
    task,
    organization_id: int,
    redis_client: async_redis.Redis
) -> List[dict]:
    """
    Deduplicate response items based on task configuration.

    Args:
        items: List of response items
        task: Task model instance with deduplication config
        organization_id: Organization ID for namespacing
        redis_client: Redis client

    Returns:
        Filtered list of unique items
    """
    if task.deduplication_strategy == 'none' or not items:
        return items

    dedup_key = f"dedup:{organization_id}:{task.id}"
    strategy = get_deduplication_strategy(
        task.deduplication_strategy,
        redis_client,
        task.deduplication_config
    )

    if not strategy:
        logger.warning(f"Could not get deduplication strategy, returning all items")
        return items

    unique_items = await strategy.filter_duplicates(items, dedup_key)
    logger.info(f"Deduplication: {len(items)} items -> {len(unique_items)} unique items")

    return unique_items


async def check_operation_idempotency(
    task_params: dict,
    task,
    redis_client: async_redis.Redis
) -> Optional[dict]:
    """
    Check if operation was already executed (idempotency check).

    Returns cached result if operation already executed, None otherwise.
    """
    if task.idempotency_strategy == 'none':
        return None

    strategy = get_idempotency_strategy(
        task.idempotency_strategy,
        redis_client,
        task.idempotency_config
    )

    if not strategy:
        return None

    # Build operation ID
    operation_id = build_operation_id(
        task_params,
        task.idempotency_config.get('key_template')
    )

    return await strategy.check_idempotency(operation_id)
