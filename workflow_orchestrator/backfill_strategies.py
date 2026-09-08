"""
Backfill strategies for generating historical data batches.

These functions generate parameter batches for backfilling workflows.
Each strategy takes a workflow and returns a list of parameter dicts.

Function signature: (workflow) -> List[Dict]
Returns: List of parameter dicts for batch execution
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)


def build_full_backfill_params(workflow) -> List[Dict]:
    """
    Full backfill with fixed batch size.

    Splits the date range into equal batches based on batch_days.

    Config example:
    {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "batch_days": 30
    }
    """
    config = workflow.backfill_config

    if not config:
        logger.warning(f"Workflow {workflow.id} has no backfill_config")
        return []

    try:
        start_date_str = config.get('start_date')
        end_date_str = config.get('end_date')
        batch_days = config.get('batch_days', 30)

        if not start_date_str or not end_date_str:
            logger.error(f"Workflow {workflow.id} missing start_date or end_date in backfill_config")
            return []

        start = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
        end = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))

        if start >= end:
            logger.error(f"Workflow {workflow.id} has invalid date range: {start} >= {end}")
            return []

        batches = []
        current = start

        while current < end:
            batch_end = min(current + timedelta(days=batch_days), end)
            batches.append({
                'params': {
                    'start_date': current.isoformat(),
                    'end_date': batch_end.isoformat()
                }
            })
            current = batch_end

        logger.info(
            f"Workflow {workflow.id} full backfill: {len(batches)} batches "
            f"from {start_date_str} to {end_date_str}"
        )
        return batches

    except (ValueError, TypeError) as e:
        logger.error(f"Workflow {workflow.id} backfill config error: {e}")
        return []


def build_date_range_backfill_params(workflow) -> List[Dict]:
    """
    Explicit date ranges for backfill.

    Allows specifying exact date ranges for each batch.
    Useful for irregular intervals or specific time periods.

    Config example:
    {
        "date_ranges": [
            {"start_date": "2024-01-01", "end_date": "2024-01-31"},
            {"start_date": "2024-02-01", "end_date": "2024-02-29"},
            {"start_date": "2024-03-01", "end_date": "2024-03-31"}
        ]
    }
    """
    config = workflow.backfill_config

    if not config:
        logger.warning(f"Workflow {workflow.id} has no backfill_config")
        return []

    date_ranges = config.get('date_ranges', [])

    if not date_ranges:
        logger.error(f"Workflow {workflow.id} has no date_ranges in backfill_config")
        return []

    batches = []
    for i, date_range in enumerate(date_ranges):
        if not isinstance(date_range, dict):
            logger.warning(f"Workflow {workflow.id} date_range {i} is not a dict, skipping")
            continue

        # Validate date range has required fields
        if 'start_date' not in date_range or 'end_date' not in date_range:
            logger.warning(f"Workflow {workflow.id} date_range {i} missing start_date or end_date")
            continue

        batches.append({'params': date_range})

    logger.info(f"Workflow {workflow.id} date_range backfill: {len(batches)} batches")
    return batches


def build_optimized_backfill_params(workflow) -> List[Dict]:
    """
    Adaptive batching based on recency.

    Uses smaller batches for recent data (more likely to change)
    and larger batches for older data (less likely to change).

    Default batch sizes:
    - Last 90 days: 7-day batches
    - 90-365 days ago: 30-day batches
    - Older than 365 days: 90-day batches

    Config example:
    {
        "start_date": "2020-01-01",
        "end_date": "2024-12-31",
        "recent_threshold_days": 90,      # Default: 90
        "medium_threshold_days": 365,     # Default: 365
        "recent_batch_days": 7,           # Default: 7
        "medium_batch_days": 30,          # Default: 30
        "old_batch_days": 90              # Default: 90
    }
    """
    config = workflow.backfill_config

    if not config:
        logger.warning(f"Workflow {workflow.id} has no backfill_config")
        return []

    try:
        start_date_str = config.get('start_date')
        end_date_str = config.get('end_date')

        if not start_date_str or not end_date_str:
            logger.error(f"Workflow {workflow.id} missing start_date or end_date in backfill_config")
            return []

        start = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
        end = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))

        if start >= end:
            logger.error(f"Workflow {workflow.id} has invalid date range: {start} >= {end}")
            return []

        # Get thresholds from config (with defaults)
        recent_threshold = config.get('recent_threshold_days', 90)
        medium_threshold = config.get('medium_threshold_days', 365)
        recent_batch_size = config.get('recent_batch_days', 7)
        medium_batch_size = config.get('medium_batch_days', 30)
        old_batch_size = config.get('old_batch_days', 90)

        now = datetime.now(start.tzinfo) if start.tzinfo else datetime.now()
        batches = []
        current = start

        while current < end:
            # Calculate days from now
            days_from_now = (now - current).days

            # Determine batch size based on recency
            if days_from_now < recent_threshold:
                batch_size = recent_batch_size
            elif days_from_now < medium_threshold:
                batch_size = medium_batch_size
            else:
                batch_size = old_batch_size

            batch_end = min(current + timedelta(days=batch_size), end)
            batches.append({
                'params': {
                    'start_date': current.isoformat(),
                    'end_date': batch_end.isoformat()
                }
            })
            current = batch_end

        logger.info(
            f"Workflow {workflow.id} optimized backfill: {len(batches)} batches "
            f"with adaptive sizing (recent={recent_batch_size}d, medium={medium_batch_size}d, old={old_batch_size}d)"
        )
        return batches

    except (ValueError, TypeError) as e:
        logger.error(f"Workflow {workflow.id} backfill config error: {e}")
        return []


def get_backfill_params(workflow) -> List[Dict]:
    """
    Router function to select backfill strategy.

    Args:
        workflow: Workflow instance with backfill_strategy and backfill_config

    Returns:
        List of parameter dicts for batch execution
    """
    strategy = workflow.backfill_strategy

    if strategy == 'full':
        return build_full_backfill_params(workflow)
    elif strategy == 'date_range':
        return build_date_range_backfill_params(workflow)
    elif strategy == 'optimized':
        return build_optimized_backfill_params(workflow)
    elif strategy == 'none':
        logger.info(f"Workflow {workflow.id} has backfill_strategy='none', no batches generated")
        return []
    else:
        logger.error(f"Workflow {workflow.id} has unknown backfill_strategy: {strategy}")
        return []
