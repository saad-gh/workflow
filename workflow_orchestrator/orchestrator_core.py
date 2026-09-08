"""
Core orchestrator logic has been refactored into the new OOP architecture.
This file is now a lightweight entry point for starting workflow runs.
"""
import asyncio
from .traverser import start_workflow_run


async def start_run_for_org(
    organization_id: int,
    schedule_id: int = None,
    workflow_id: int = None,
    redis_client = None # Kept for compatibility, but not used in the new architecture
) -> str:
    """
    Start a workflow run for an organization asynchronously.
    """
    run_id = await start_workflow_run(organization_id, schedule_id, workflow_id)
    return run_id


async def ensure_default_gatekeepers():
    from .models import Workflow, Task, WorkflowContinuation, Attribute, TaskAttribute
    gatekeeper, _ = Task.objects.get_or_create(
        slug="global_require_success",
        defaults={"name": "Require Parent Success", "task_type": Task.TASK_TYPE_MUTATION},
    )
    _set_attr_sync(gatekeeper, "mutations", {
        "result": "{{ parent.status }}",
        "skip": "{{ parent.status != \"success\" }}",
    })
    return gatekeeper


def _set_attr_sync(task_obj, name, value):
    from .models import Attribute, TaskAttribute
    attr, _ = Attribute.objects.get_or_create(name=name, value=value)
    TaskAttribute.objects.get_or_create(task=task_obj, attribute=attr)
