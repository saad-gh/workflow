"""
Jinja2 templating for task attributes.

Allows dynamic task attributes based on parent task results, workflow context, and runtime data.
"""
import json
import logging
from typing import Dict, Any, Optional, List
from jinja2 import Template, Environment, StrictUndefined, TemplateSyntaxError, UndefinedError

logger = logging.getLogger(__name__)


def render_task_attributes(
    attr_map: Dict[str, Any],
    parent_results: Optional[List[dict]] = None,
    workflow_context: Optional[dict] = None,
    previous_results: Optional[dict] = None,
    extra_context: Optional[dict] = None
) -> Dict[str, Any]:
    """
    Render task attributes using Jinja2 templates.

    Args:
        attr_map: Task attributes dict (url, headers, params, body, etc.)
        parent_results: List of parent task result documents
        workflow_context: Workflow context (org_id, last_sync_timestamp, etc.)
        previous_results: Dict mapping task_slug -> result_doc
        extra_context: Extra variables to merge directly into template context

    Returns:
        Rendered attributes dict with templates resolved
    """
    if not parent_results:
        parent_results = []

    if not workflow_context:
        workflow_context = {}

    # Build template context
    context = {
        'parents': parent_results,
        'parent': parent_results[0] if parent_results else {},
        'workflow': workflow_context,
        'last_sync_timestamp': workflow_context.get('last_sync_timestamp'),
        'organization_id': workflow_context.get('organization_id'),
        'service': workflow_context.get('service', {}),
        'tasks': previous_results or {},
    }

    if extra_context:
        context.update(extra_context)

    if workflow_context and workflow_context.get("payload"):
        context["payload"] = workflow_context["payload"]

    # Render each attribute
    rendered = {}

    for key, value in attr_map.items():
        rendered[key] = _render_value(value, context)

    return rendered


def _render_value(value: Any, context: dict) -> Any:
    """
    Recursively render a value (string, dict, list) with Jinja2.
    """
    if isinstance(value, str):
        return _render_string(value, context)
    elif isinstance(value, dict):
        return {k: _render_value(v, context) for k, v in value.items()}
    elif isinstance(value, list):
        return [_render_value(item, context) for item in value]
    else:
        return value


def _render_string(template_str: str, context: dict) -> str:
    """
    Render a single string template.
    Returns original string if no template markers found or if rendering fails.
    """
    # Quick check: if no template markers, return as-is
    if '{{' not in template_str and '{%' not in template_str:
        return template_str

    try:
        env = get_template_env()
        template = env.from_string(template_str)
        return template.render(**context)
    except (TemplateSyntaxError, UndefinedError) as e:
        logger.warning(f"Template rendering failed for '{template_str}': {e}")
        return template_str
    except Exception as e:
        logger.error(f"Unexpected error rendering template '{template_str}': {e}", exc_info=True)
        return template_str


def render_url_with_parent_data(url_template: str, parent_result: dict) -> str:
    """
    Convenience function to render URL with parent result data.

    Example:
        url_template = "/api/users/{{parent.response_json.user_id}}/orders"
        parent_result = {"response_json": {"user_id": 123}}
        => "/api/users/123/orders"
    """
    return _render_string(url_template, {'parent': parent_result})


def _inject_service_context(workflow, context: dict) -> None:
    svc = workflow.task.effective_service
    if svc:
        from .conf import get_service_oauth_config
        context['service'] = {
            'name': svc.name,
            'metadata': svc.metadata,
            **get_service_oauth_config(svc),
        }
    else:
        context['service'] = {}


def build_template_context_for_sync(workflow, last_sync_timestamp=None) -> dict:
    """
    Build template context for sync-type workflows.

    Includes:
    - last_sync_timestamp for incremental sync
    - organization metadata
    - workflow metadata
    """
    context = {
        'workflow_type': workflow.workflow_type,
        'organization_id': workflow.organization_id,
        'last_sync_timestamp': last_sync_timestamp or workflow.last_sync_timestamp,
        'workflow_metadata': workflow.metadata,
    }

    # Add organization-specific context if available
    if hasattr(workflow.organization, 'metadata'):
        context['organization_metadata'] = workflow.organization.metadata

    _inject_service_context(workflow, context)
    return context


def build_template_context_for_migration(workflow) -> dict:
    """
    Build template context for migration-type workflows.

    Includes:
    - migration progress (last processed ID, count, etc.)
    - organization metadata
    """
    context = {
        'workflow_type': workflow.workflow_type,
        'organization_id': workflow.organization_id,
        'migration_progress': workflow.migration_progress,
        'migration_completed': workflow.migration_completed,
        'workflow_metadata': workflow.metadata,
    }
    _inject_service_context(workflow, context)
    return context


def build_template_context_with_webhook(
    workflow,
    webhook_payload: Optional[dict] = None,
    parent_results: Optional[List[dict]] = None
) -> dict:
    """
    Build template context including webhook payload or generic payload.

    Context structure:
    {
        'webhook': {...},              # Webhook-specific context (if webhook)
        'payload': {...},              # Generic payload context (always available if provided)
        'parent': {...},               # Parent task result
        'parents': [...],              # All parent results
        'workflow': {...},             # Workflow context
        'organization_id': 123
    }
    """
    context = {}

    if webhook_payload:
        generic_payload = webhook_payload.get("payload", {}) if "payload" in webhook_payload and "events" not in webhook_payload else webhook_payload

        if isinstance(webhook_payload, dict) and webhook_payload.get("events"):
            context['webhook'] = {
                'payload': webhook_payload,
                'event_type': webhook_payload.get('event_type') or webhook_payload.get('type'),
                'timestamp': webhook_payload.get('timestamp') or webhook_payload.get('created_at'),
                'id': webhook_payload.get('id'),
                'resource_id': webhook_payload.get('resource_id') or webhook_payload.get('object_id'),
            }
            context['payload'] = webhook_payload
        else:
            context['payload'] = generic_payload

    # Add parent results
    if parent_results:
        context['parents'] = parent_results
        context['parent'] = parent_results[0] if parent_results else {}

    # Add workflow context
    if workflow.workflow_type == 'sync':  # Workflow.TYPE_SYNC
        workflow_ctx = build_template_context_for_sync(workflow)
        context.update(workflow_ctx)
    elif workflow.workflow_type == 'migration':  # Workflow.TYPE_MIGRATION
        workflow_ctx = build_template_context_for_migration(workflow)
        context.update(workflow_ctx)
    else:
        context['workflow'] = {
            'workflow_type': workflow.workflow_type,
            'organization_id': workflow.organization_id,
        }

    _inject_service_context(workflow, context)
    return context


# ============================================
# Template Helpers (can be used in templates)
# ============================================

def format_date(date_obj, fmt='%Y-%m-%d'):
    """
    Format date for use in templates.
    Usage: {{ last_sync_timestamp | format_date('%Y-%m-%dT%H:%M:%SZ') }}
    """
    if not date_obj:
        return ''
    if isinstance(date_obj, str):
        return date_obj
    return date_obj.strftime(fmt)


def extract_link_header(link_header, rel="next") -> str:
    """
    Parse RFC 5988 Link headers and return the URL for the specified rel.
    Usage: {{ result.response_headers['Link'] | extract_link_header('next') }}
    """
    if not link_header:
        return ""
    parts = link_header.split(",")
    for part in parts:
        section = part.split(";")
        if len(section) < 2:
            continue
        url_part = section[0].strip().lstrip("<").rstrip(">")
        rel_part = section[1].strip()
        if f'rel="{rel}"' in rel_part or f"rel='{rel}'" in rel_part or f"rel={rel}" in rel_part:
            return url_part
    return ""


# Register custom filters for Jinja2
def get_template_env() -> Environment:
    """
    Get Jinja2 environment with custom filters.
    """
    env = Environment(undefined=StrictUndefined)
    env.filters['format_date'] = format_date
    env.filters['extract_link_header'] = extract_link_header
    return env
