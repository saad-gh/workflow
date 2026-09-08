"""
Data Egress Tracking for Cost Modeling

This module tracks API data egress (request + response sizes) for billing and cost analysis.
Data is stored in both Redis (real-time aggregates) and Database (permanent per-request logs).
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger(__name__)


def _get_date_key() -> str:
    """Get current date key (YYYY-MM-DD) for daily aggregates"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _get_month_key() -> str:
    """Get current month key (YYYY-MM) for monthly aggregates"""
    return datetime.now(timezone.utc).strftime("%Y-%m")


async def track_data_egress(
    task_id: str,
    workflow_id: int,
    credential_id: int,
    organization_id: int,
    service_id: int,
    request_size_bytes: int,
    response_size_bytes: int,
    url: str,
    method: str,
    status_code: int,
    run_id: str,
    redis
) -> Dict[str, Any]:
    """
    Track data egress for an API request.
    Updates Redis counters (real-time) and queues DB write (permanent storage).

    Args:
        task_id: Task execution ID
        workflow_id: Workflow ID
        credential_id: Credential ID
        organization_id: Organization ID
        service_id: Service ID
        request_size_bytes: Request body size in bytes
        response_size_bytes: Response body size in bytes
        url: API endpoint
        method: HTTP method
        status_code: HTTP status code
        run_id: Workflow run ID
        redis: Async Redis connection

    Returns:
        Dict with tracking info and updated counters
    """
    total_bytes = request_size_bytes + response_size_bytes
    date_key = _get_date_key()
    month_key = _get_month_key()

    egress_info = {
        "task_id": task_id,
        "request_size_bytes": request_size_bytes,
        "response_size_bytes": response_size_bytes,
        "total_bytes": total_bytes,
        "tracked_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        # Update Redis counters (real-time aggregates)

        # Per-credential counters
        cred_daily_key = f"egress:cred:{credential_id}:daily:{date_key}:bytes"
        cred_monthly_key = f"egress:cred:{credential_id}:monthly:{month_key}:bytes"

        await redis.incrby(cred_daily_key, total_bytes)
        await redis.incrby(cred_monthly_key, total_bytes)

        # Set TTL (60 days for daily, 2 years for monthly)
        await redis.expire(cred_daily_key, 60 * 86400)
        await redis.expire(cred_monthly_key, 730 * 86400)

        # Per-organization counters
        org_daily_key = f"egress:org:{organization_id}:daily:{date_key}:bytes"
        org_monthly_key = f"egress:org:{organization_id}:monthly:{month_key}:bytes"

        await redis.incrby(org_daily_key, total_bytes)
        await redis.incrby(org_monthly_key, total_bytes)

        await redis.expire(org_daily_key, 60 * 86400)
        await redis.expire(org_monthly_key, 730 * 86400)

        # Per-service counters (for all orgs using this service)
        service_daily_key = f"egress:service:{service_id}:daily:{date_key}:bytes"
        service_monthly_key = f"egress:service:{service_id}:monthly:{month_key}:bytes"

        await redis.incrby(service_daily_key, total_bytes)
        await redis.incrby(service_monthly_key, total_bytes)

        await redis.expire(service_daily_key, 60 * 86400)
        await redis.expire(service_monthly_key, 730 * 86400)

        # Get updated totals for response
        cred_daily_total = await redis.get(cred_daily_key)
        org_daily_total = await redis.get(org_daily_key)

        egress_info["credential_daily_total"] = int(cred_daily_total) if cred_daily_total else 0
        egress_info["organization_daily_total"] = int(org_daily_total) if org_daily_total else 0

        # Queue for database write (async, non-blocking)
        # Store request details in Redis list for batch processing
        egress_record = {
            "task_id": task_id,
            "workflow_id": workflow_id,
            "credential_id": credential_id,
            "organization_id": organization_id,
            "service_id": service_id,
            "request_size_bytes": request_size_bytes,
            "response_size_bytes": response_size_bytes,
            "total_size_bytes": total_bytes,
            "url": url,
            "method": method,
            "status_code": status_code,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        import json
        egress_queue_key = "egress:db_write_queue"
        await redis.rpush(egress_queue_key, json.dumps(egress_record))

        logger.info(
            f"Data egress tracked: {total_bytes} bytes "
            f"(req: {request_size_bytes}, resp: {response_size_bytes}) "
            f"for task {task_id}"
        )

    except Exception as e:
        logger.error(f"Error tracking data egress: {e}", exc_info=True)
        egress_info["error"] = str(e)

    return egress_info


async def get_egress_stats(
    credential_id: int = None,
    organization_id: int = None,
    service_id: int = None,
    period: str = "daily",
    redis = None
) -> Dict[str, Any]:
    """
    Get data egress statistics from Redis.

    Args:
        credential_id: Filter by credential
        organization_id: Filter by organization
        service_id: Filter by service
        period: "daily" or "monthly"
        redis: Redis connection

    Returns:
        Dict with egress statistics
    """
    if period not in ["daily", "monthly"]:
        raise ValueError("Period must be 'daily' or 'monthly'")

    date_key = _get_date_key() if period == "daily" else _get_month_key()
    stats = {}

    try:
        if credential_id:
            key = f"egress:cred:{credential_id}:{period}:{date_key}:bytes"
            value = await redis.get(key)
            stats["credential_bytes"] = int(value) if value else 0

        if organization_id:
            key = f"egress:org:{organization_id}:{period}:{date_key}:bytes"
            value = await redis.get(key)
            stats["organization_bytes"] = int(value) if value else 0

        if service_id:
            key = f"egress:service:{service_id}:{period}:{date_key}:bytes"
            value = await redis.get(key)
            stats["service_bytes"] = int(value) if value else 0

        stats["period"] = period
        stats["date_key"] = date_key

    except Exception as e:
        logger.error(f"Error getting egress stats: {e}")
        stats["error"] = str(e)

    return stats


def process_egress_db_write_queue():
    """
    Process queued egress records and write to database.
    This should be called periodically (e.g., every minute) by an RQ scheduled task.

    Batch processes up to 1000 records per run to avoid blocking.
    """
    import json
    from django.db import transaction
    import redis
    from .models import DataEgressLog, Workflow

    rdb = redis.Redis(
        host=__import__('os').environ.get('REDIS_HOST', 'localhost'),
        port=int(__import__('os').environ.get('REDIS_PORT', 6379)),
        db=int(__import__('os').environ.get('REDIS_DB', 0)),
        password=__import__('os').environ.get('REDIS_AUTH'),
        decode_responses=True
    )

    egress_queue_key = "egress:db_write_queue"
    batch_size = 1000
    records_processed = 0

    try:
        with transaction.atomic():
            egress_logs = []

            for _ in range(batch_size):
                record_json = rdb.lpop(egress_queue_key)
                if not record_json:
                    break

                try:
                    record = json.loads(record_json)

                    # Create DataEgressLog instance
                    egress_log = DataEgressLog(
                        task_id=record["task_id"],
                        workflow_id=record["workflow_id"],
                        credential_id=record["credential_id"],
                        organization_id=record["organization_id"],
                        service_id=record["service_id"],
                        request_size_bytes=record["request_size_bytes"],
                        response_size_bytes=record["response_size_bytes"],
                        url=record.get("url", ""),
                        method=record.get("method", ""),
                        status_code=record.get("status_code"),
                        run_id=record["run_id"]
                    )
                    egress_logs.append(egress_log)
                    records_processed += 1

                except Exception as e:
                    logger.error(f"Error parsing egress record: {e}")
                    continue

            # Bulk create for efficiency
            if egress_logs:
                DataEgressLog.objects.bulk_create(egress_logs, batch_size=1000)
                logger.info(f"Wrote {len(egress_logs)} egress records to database")

    except Exception as e:
        logger.error(f"Error processing egress DB write queue: {e}", exc_info=True)

    return records_processed
