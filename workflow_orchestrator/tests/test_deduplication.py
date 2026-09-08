"""
Tests for deduplication strategies (strategies.py).

Covers:
  - DedupById: marks seen by ID field; skips duplicate
  - DedupByCompositeKey: marks by multiple fields
  - DedupByHash: marks by content hash; identical item skipped, changed item passes
  - filter_duplicates: mixed list → only new items returned
  - get_deduplication_strategy factory: unknown name → None
  - DEDUP_NONE skips dedup entirely (all items pass)

Uses a fake async Redis client (no real Redis required).
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

from django.test import TestCase

from workflow_orchestrator.strategies import (
    DedupById,
    DedupByCompositeKey,
    DedupByHash,
    deduplicate_response_items,
    get_deduplication_strategy,
)
from workflow_orchestrator.tests.base import make_task


# ---------------------------------------------------------------------------
# Fake async Redis
# ---------------------------------------------------------------------------

class FakeRedis:
    """In-memory async Redis stand-in for tests."""

    def __init__(self):
        self._store: dict = {}

    async def exists(self, key):
        return 1 if key in self._store else 0

    async def setex(self, key, ttl, value):
        self._store[key] = value

    async def get(self, key):
        return self._store.get(key)

    def reset(self):
        self._store.clear()


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# DedupById
# ---------------------------------------------------------------------------

class DedupByIdTest(TestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.strategy = DedupById(self.redis, {'key_fields': ['id']})

    def test_new_item_not_duplicate(self):
        result = run(self.strategy.is_duplicate({'id': 1}, 'dedup:test'))
        self.assertFalse(result)

    def test_seen_item_is_duplicate(self):
        run(self.strategy.mark_as_seen({'id': 1}, 'dedup:test'))
        result = run(self.strategy.is_duplicate({'id': 1}, 'dedup:test'))
        self.assertTrue(result)

    def test_different_ids_not_duplicate(self):
        run(self.strategy.mark_as_seen({'id': 1}, 'dedup:test'))
        result = run(self.strategy.is_duplicate({'id': 2}, 'dedup:test'))
        self.assertFalse(result)

    def test_missing_id_field_not_duplicate(self):
        """Items without the ID field are never marked as duplicate."""
        result = run(self.strategy.is_duplicate({'name': 'no-id'}, 'dedup:test'))
        self.assertFalse(result)

    def test_filter_duplicates_removes_seen(self):
        items = [{'id': 1}, {'id': 2}, {'id': 3}]
        run(self.strategy.mark_as_seen({'id': 2}, 'dedup:test'))

        unique = run(self.strategy.filter_duplicates(items, 'dedup:test'))
        ids = [i['id'] for i in unique]
        self.assertNotIn(2, ids)
        self.assertIn(1, ids)
        self.assertIn(3, ids)

    def test_filter_duplicates_marks_new_as_seen(self):
        items = [{'id': 10}]
        run(self.strategy.filter_duplicates(items, 'dedup:test'))
        # Should be marked now
        is_dup = run(self.strategy.is_duplicate({'id': 10}, 'dedup:test'))
        self.assertTrue(is_dup)


# ---------------------------------------------------------------------------
# DedupByCompositeKey
# ---------------------------------------------------------------------------

class DedupByCompositeKeyTest(TestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.strategy = DedupByCompositeKey(
            self.redis, {'composite_fields': ['org_id', 'external_id']}
        )

    def test_new_composite_key_not_duplicate(self):
        item = {'org_id': 1, 'external_id': 'EXT-001'}
        result = run(self.strategy.is_duplicate(item, 'dedup:test'))
        self.assertFalse(result)

    def test_seen_composite_key_is_duplicate(self):
        item = {'org_id': 1, 'external_id': 'EXT-001'}
        run(self.strategy.mark_as_seen(item, 'dedup:test'))
        result = run(self.strategy.is_duplicate(item, 'dedup:test'))
        self.assertTrue(result)

    def test_different_org_not_duplicate(self):
        item1 = {'org_id': 1, 'external_id': 'EXT-001'}
        item2 = {'org_id': 2, 'external_id': 'EXT-001'}
        run(self.strategy.mark_as_seen(item1, 'dedup:test'))
        result = run(self.strategy.is_duplicate(item2, 'dedup:test'))
        self.assertFalse(result)

    def test_empty_composite_fields_never_duplicate(self):
        strategy = DedupByCompositeKey(self.redis, {'composite_fields': []})
        result = run(strategy.is_duplicate({'any': 'value'}, 'dedup:test'))
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# DedupByHash
# ---------------------------------------------------------------------------

class DedupByHashTest(TestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.strategy = DedupByHash(self.redis, {})

    def test_identical_item_duplicate(self):
        item = {'name': 'alice', 'age': 30}
        run(self.strategy.mark_as_seen(item, 'dedup:hash'))
        result = run(self.strategy.is_duplicate(item, 'dedup:hash'))
        self.assertTrue(result)

    def test_changed_item_not_duplicate(self):
        item1 = {'name': 'alice', 'age': 30}
        item2 = {'name': 'alice', 'age': 31}  # age changed
        run(self.strategy.mark_as_seen(item1, 'dedup:hash'))
        result = run(self.strategy.is_duplicate(item2, 'dedup:hash'))
        self.assertFalse(result)

    def test_key_order_independent(self):
        """SHA256 hash must be consistent regardless of dict key order."""
        item_a = {'b': 2, 'a': 1}
        item_b = {'a': 1, 'b': 2}
        hash_a = self.strategy._compute_hash(item_a)
        hash_b = self.strategy._compute_hash(item_b)
        self.assertEqual(hash_a, hash_b)


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

class GetDeduplicationStrategyTest(TestCase):

    def setUp(self):
        self.redis = FakeRedis()

    def test_by_id_returns_dedup_by_id(self):
        strategy = get_deduplication_strategy('by_id', self.redis, {'key_fields': ['id']})
        self.assertIsInstance(strategy, DedupById)

    def test_by_composite_key_returns_correct_type(self):
        strategy = get_deduplication_strategy(
            'by_composite_key', self.redis, {'composite_fields': ['a', 'b']}
        )
        self.assertIsInstance(strategy, DedupByCompositeKey)

    def test_by_hash_returns_correct_type(self):
        strategy = get_deduplication_strategy('by_hash', self.redis, {})
        self.assertIsInstance(strategy, DedupByHash)

    def test_unknown_strategy_returns_none(self):
        strategy = get_deduplication_strategy('nonexistent', self.redis, {})
        self.assertIsNone(strategy)


# ---------------------------------------------------------------------------
# deduplicate_response_items (high-level helper)
# ---------------------------------------------------------------------------

class DeduplicateResponseItemsTest(TestCase):

    def _make_task(self, strategy, config=None):
        task = make_task(f'dedup_task_{strategy}')
        task.deduplication_strategy = strategy
        task.deduplication_config = config or {}
        task.save()
        return task

    def test_none_strategy_returns_all_items(self):
        task = self._make_task('none')
        items = [{'id': 1}, {'id': 2}, {'id': 3}]
        redis = FakeRedis()
        result = run(deduplicate_response_items(items, task, organization_id=1, redis_client=redis))
        self.assertEqual(result, items)

    def test_by_id_filters_duplicates(self):
        task = self._make_task('by_id', {'key_fields': ['id']})
        redis = FakeRedis()

        # First run: all 3 items are new
        items = [{'id': 1}, {'id': 2}, {'id': 3}]
        result1 = run(deduplicate_response_items(items, task, organization_id=1, redis_client=redis))
        self.assertEqual(len(result1), 3)

        # Second run: all 3 items are duplicates
        result2 = run(deduplicate_response_items(items, task, organization_id=1, redis_client=redis))
        self.assertEqual(len(result2), 0)

    def test_by_id_different_orgs_not_shared(self):
        """Dedup keys are namespaced by org_id — different orgs don't share state."""
        task = self._make_task('by_id', {'key_fields': ['id']})
        redis = FakeRedis()

        items = [{'id': 1}]
        run(deduplicate_response_items(items, task, organization_id=1, redis_client=redis))

        # Same item but different org — should NOT be filtered
        result = run(deduplicate_response_items(items, task, organization_id=2, redis_client=redis))
        self.assertEqual(len(result), 1)

    def test_empty_list_returns_empty(self):
        task = self._make_task('by_id', {'key_fields': ['id']})
        redis = FakeRedis()
        result = run(deduplicate_response_items([], task, organization_id=1, redis_client=redis))
        self.assertEqual(result, [])
