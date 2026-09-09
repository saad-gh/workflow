"""
Quota Management for API Rate Limiting

This module handles checking and enforcing API quotas at the credential level.
Quotas are inherited from Service.limits and can be restricted per-workflow.

Quota enforcement is per-credential (respects real API limits), but distribution
is automatic via priority queuing (high-priority workflows get first access).
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Tuple, Any, Optional
import json

logger = logging.getLogger(__name__)

async def _get_quota_rules(credential_id: int, service_id: int, redis) -> list:
    from .conf import get_credential_model
    Credential = get_credential_model()
    credential = await Credential.objects.aget(id=credential_id)

    if credential.limits and credential.limits.get('quota_rules'):
        return credential.limits.get('quota_rules'), credential.limits

    config_key = f"quota:config:service:{service_id}"
    cached_config = await redis.get(config_key)
    if cached_config:
        service_limits = json.loads(cached_config)
    else:
        from .conf import get_service_model
        Service = get_service_model()
        service = await Service.objects.filter(id=service_id).afirst()
        service_limits = service.limits if (service and service.limits) else {}
        await redis.setex(config_key, 3600, json.dumps(service_limits))
    
    return service_limits.get('quota_rules', []), service_limits


def _get_window_start(rule_type: str) -> int:
    """
    Calculate window start timestamp based on rule type.
    All windows align to calendar boundaries in UTC.

    Args:
        rule_type: "hourly", "daily", or "monthly"

    Returns:
        Unix timestamp of window start
    """
    now = datetime.now(timezone.utc)

    if rule_type == "hourly":
        # Truncate to hour start (e.g., 14:23:45 → 14:00:00)
        window_start = now.replace(minute=0, second=0, microsecond=0)
    elif rule_type == "daily":
        # Truncate to midnight UTC
        window_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif rule_type == "monthly":
        # Truncate to 1st of month
        window_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unknown rule type: {rule_type}")

    return int(window_start.timestamp())


def _get_window_key(rule: Dict[str, Any], credential_id: int, service_id: int) -> str:
    """
    Generate Redis key for quota window.

    Args:
        rule: Quota rule dict with 'type' and 'scope'
        credential_id: Credential ID
        service_id: Service ID

    Returns:
        Redis key string
    """
    rule_type = rule['type']
    scope = rule.get('scope', 'per_credential')
    window_ts = _get_window_start(rule_type)

    if scope == 'account_wide':
        # All credentials for this service share quota
        return f"quota:service:{service_id}:{rule_type}:{window_ts}:count"
    else:
        # Per-credential quota
        return f"quota:cred:{credential_id}:{rule_type}:{window_ts}:count"


async def check_and_enforce_quota(
    credential_id: int,
    service_id: int,
    workflow_quota_limit: Optional[Dict[str, int]],
    redis
) -> Tuple[bool, Dict[str, Any]]:
    """
    Check if credential/workflow has quota remaining.

    Args:
        credential_id: Credential ID
        service_id: Service ID (to look up quota rules)
        workflow_quota_limit: Optional per-workflow quota limits (e.g., {"daily_limit": 500})
        redis: Async Redis connection

    Returns:
        (quota_ok, quota_info) tuple
        - quota_ok: True if quota available, False if exceeded
        - quota_info: Dict with usage details and which rule was exceeded
    """
    quota_info = {
        "credential_id": credential_id,
        "service_id": service_id,
        "rules_checked": [],
        "exceeded_rule": None
    }

    try:
        quota_rules, _ = await _get_quota_rules(credential_id, service_id, redis)

        if not quota_rules:
            # No quota enforcement configured
            return True, quota_info

        # Check each quota rule
        for rule in quota_rules:
            rule_type = rule['type']  # 'hourly', 'daily', 'monthly'
            rule_limit = rule['limit']
            rule_scope = rule.get('scope', 'per_credential')

            # Check if workflow has more restrictive limit
            if workflow_quota_limit and f"{rule_type}_limit" in workflow_quota_limit:
                workflow_limit = workflow_quota_limit[f"{rule_type}_limit"]
                rule_limit = min(rule_limit, workflow_limit)  # Use most restrictive

            # Get current usage
            window_key = _get_window_key(rule, credential_id, service_id)
            current_count = await redis.get(window_key)
            current_count = int(current_count) if current_count else 0

            remaining = rule_limit - current_count
            usage_percent = (current_count / rule_limit * 100) if rule_limit > 0 else 0

            rule_info = {
                "type": rule_type,
                "scope": rule_scope,
                "limit": rule_limit,
                "current": current_count,
                "remaining": remaining,
                "usage_percent": round(usage_percent, 2)
            }
            quota_info["rules_checked"].append(rule_info)

            # Check if exceeded
            if current_count >= rule_limit:
                quota_info["exceeded_rule"] = rule_info
                logger.warning(
                    f"Quota exceeded for credential {credential_id}: "
                    f"{rule_type} limit {rule_limit} reached ({current_count} requests)"
                )
                return False, quota_info

        # All quotas OK
        return True, quota_info

    except Exception as e:
        # Fail open - if quota check errors, allow task to proceed
        logger.error(f"Error checking quota for credential {credential_id}: {e}", exc_info=True)
        quota_info["error"] = str(e)
        return True, quota_info


async def increment_api_usage(
    credential_id: int,
    service_id: int,
    redis
) -> Dict[str, Any]:
    """
    Increment API usage counters for all applicable quota windows.
    Also check warning thresholds and log warnings.

    Args:
        credential_id: Credential ID
        service_id: Service ID
        redis: Async Redis connection

    Returns:
        Dict with updated quota info
    """
    quota_info = {
        "credential_id": credential_id,
        "service_id": service_id,
        "incremented": []
    }

    try:
        quota_rules, service_limits = await _get_quota_rules(credential_id, service_id, redis)
        warning_thresholds = service_limits.get('warning_thresholds', [80, 90, 95])

        # Increment each quota counter atomically
        for rule in quota_rules:
            rule_type = rule['type']
            rule_limit = rule['limit']

            window_key = _get_window_key(rule, credential_id, service_id)

            # Atomic increment
            new_count = await redis.incr(window_key)

            # Set TTL if this is first increment (2x window duration for safety)
            if new_count == 1:
                window_seconds = {
                    'hourly': 3600,
                    'daily': 86400,
                    'monthly': 2592000
                }.get(rule_type, 86400)
                await redis.expire(window_key, window_seconds * 2)

            usage_percent = (new_count / rule_limit * 100) if rule_limit > 0 else 0

            quota_info["incremented"].append({
                "type": rule_type,
                "count": new_count,
                "limit": rule_limit,
                "usage_percent": round(usage_percent, 2)
            })

            # Check warning thresholds
            window_ts = _get_window_start(rule_type)
            warned_key = f"quota:cred:{credential_id}:{rule_type}:{window_ts}:warned"

            for threshold in warning_thresholds:
                if usage_percent >= threshold:
                    # Check if we've already warned at this threshold
                    already_warned = await redis.sismember(warned_key, str(threshold))
                    if not already_warned:
                        logger.warning(
                            f"Quota warning: Credential {credential_id} reached {threshold}% "
                            f"of {rule_type} limit ({new_count}/{rule_limit})"
                        )
                        await redis.sadd(warned_key, str(threshold))
                        # Set TTL on warned set
                        await redis.expire(warned_key, 86400 * 2)

        return quota_info

    except Exception as e:
        logger.error(f"Error incrementing quota for credential {credential_id}: {e}", exc_info=True)
        quota_info["error"] = str(e)
        return quota_info
