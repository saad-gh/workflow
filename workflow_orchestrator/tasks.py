"""
RQ (Redis Queue) background tasks for workflow execution.
"""
import asyncio
import logging
from typing import Optional

import django_rq
from django.conf import settings

from .traverser import start_workflow_run, resume_run

logger = logging.getLogger(__name__)

def execute_workflow_run(organization_id: int, schedule_id: Optional[int] = None, workflow_id: Optional[int] = None):
    """
    RQ task: Start a complete workflow run for an organization.
    """
    try:
        logger.info(f"Starting workflow run for org:{organization_id} schedule:{schedule_id}")
        run_id = asyncio.run(start_workflow_run(organization_id, schedule_id, workflow_id))
        logger.info(f"Workflow run completed for org:{organization_id} schedule:{schedule_id}. run_id:{run_id}")
        return run_id
    except Exception as e:
        logger.error(f"Error in execute_workflow_run: {str(e)}", exc_info=True)
        raise

def resume_workflow_run(checkpoint_id: int):
    """
    RQ task: Resume a workflow run from a checkpoint.
    """
    try:
        logger.info(f"Resuming workflow run from checkpoint:{checkpoint_id}")
        run_id = asyncio.run(resume_run(checkpoint_id))
        logger.info(f"Resumed workflow run from checkpoint:{checkpoint_id}. run_id:{run_id}")
        return run_id
    except Exception as e:
        logger.error(f"Error in resume_workflow_run: {str(e)}", exc_info=True)
        raise

def sync_egress_logs_to_database():
    """
    RQ Scheduled Task: Sync data egress logs from Redis queue to database.
    """
    from .data_egress_manager import process_egress_db_write_queue

    try:
        records_processed = process_egress_db_write_queue()
        logger.info(f"Synced {records_processed} egress records to database")
        return records_processed
    except Exception as e:
        logger.error(f"Error syncing egress logs to database: {e}", exc_info=True)
        raise

def redis_memory_cleanup():
    """
    RQ Scheduled Task: Cleanup expired Redis keys and enforce TTLs.
    """
    from .redis_memory_manager import redis_cleanup_task

    try:
        result = redis_cleanup_task()
        logger.info(
            f"Redis cleanup complete: "
            f"deleted={result['deleted']['total']}, "
            f"ttls_updated={result['ttls_updated']}, "
            f"health={result['memory_health']}"
        )
        return result
    except Exception as e:
        logger.error(f"Error in Redis memory cleanup: {e}", exc_info=True)
        raise
