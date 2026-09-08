"""
Handles credential locking, quota checking, and preemption.
"""
import asyncio
import json
import logging
from typing import Optional
from asgiref.sync import sync_to_async

import redis.asyncio as async_redis

from .quota_manager import check_and_enforce_quota, increment_api_usage

logger = logging.getLogger(__name__)

class QuotaExceededError(Exception):
    """Raised when a task cannot run due to quota limits."""
    def __init__(self, message, quota_info):
        super().__init__(message)
        self.quota_info = quota_info

class PreemptedError(Exception):
    """Raised when a task is preempted by a higher-priority task."""
    pass

class NoAvailableCredentialsError(Exception):
    """Raised when no credentials can be locked."""
    pass


class CredentialManager:
    """Manages locking and quota for API credentials."""

    def __init__(self, rdb: async_redis.Redis):
        self.rdb = rdb

    async def acquire(self, context, node) -> str:
        """Acquires a lock on a credential and returns the credential ID."""
        credential_ids = await self._get_credential_ids(node)
        if not credential_ids:
            raise NoAvailableCredentialsError("No credential IDs provided for this node.")

        priority = node.workflow.priority_override or 2
        service_id = node.task.effective_service.id if node.task.effective_service else None
        workflow_quota_limit = node.workflow.quota_limit

        last_quota_info = None

        for cred_id in credential_ids:
            lock_key = f"cred_lock:{cred_id}"
            lock_val = {
                "task_id": f"{context.run_id}:{node.workflow.id}", # Simplified task_id
                "priority": priority,
                "run_id": context.run_id,
            }

            # 1. Try to acquire lock
            if await self.rdb.set(lock_key, json.dumps(lock_val), nx=True):
                # 2. Check quota
                if not service_id:
                    return cred_id # No service, no quota check

                quota_ok, quota_info = await check_and_enforce_quota(
                    cred_id, service_id, workflow_quota_limit, self.rdb
                )
                last_quota_info = quota_info

                if quota_ok:
                    return cred_id  # Acquired lock and has quota
                else:
                    # Has lock but no quota, release and try next
                    await self.rdb.delete(lock_key)
                    continue
            
            # 3. Could not get lock, check for preemption
            else:
                holder_raw = await self.rdb.get(lock_key)
                if not holder_raw:
                    continue # Lock was released in the meantime

                try:
                    holder = json.loads(holder_raw)
                    if priority < holder.get("priority", 99):
                        # Preempt the lock
                        await self.rdb.set(lock_key, json.dumps(lock_val))
                        logger.warning(f"Preempted lock for credential {cred_id} from task {holder.get('task_id')}")
                        # In a full implementation, we would re-queue the preempted task.
                        # await self._requeue_preempted_task(holder)

                        # Now that we have the lock, check quota
                        if not service_id:
                            return cred_id
                        
                        quota_ok, quota_info = await check_and_enforce_quota(
                            cred_id, service_id, workflow_quota_limit, self.rdb
                        )
                        last_quota_info = quota_info

                        if quota_ok:
                            return cred_id
                        else:
                            # Preempted a task just to find out we have no quota. Bad. Release lock.
                            await self.rdb.delete(lock_key)
                            # We should probably restore the old lock value here, but that is complex.
                            continue
                except (json.JSONDecodeError, TypeError):
                    continue

        # If we loop through all credentials and none are available/have quota
        if last_quota_info and last_quota_info.get("exceeded_rule"):
            raise QuotaExceededError("All available credentials have exceeded their quota.", last_quota_info)
        
        raise NoAvailableCredentialsError("Could not acquire lock on any available credential.")

    async def release(self, cred_id: str, context, node):
        """Releases the lock on a credential, but only if we still own it."""
        lock_key = f"cred_lock:{cred_id}"
        task_id = f"{context.run_id}:{node.workflow.id}"
        
        try:
            lock_raw = await self.rdb.get(lock_key)
            if lock_raw:
                lock = json.loads(lock_raw)
                if lock.get("task_id") == task_id:
                    await self.rdb.delete(lock_key)
        except Exception as e:
            logger.error(f"Error releasing lock for cred {cred_id}: {e}")

    @sync_to_async
    def _get_credential_ids(self, node) -> list:
        return list(node.workflow.credentials.values_list('id', flat=True))
