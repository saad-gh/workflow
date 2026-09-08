"""
Fixture factories for workflow run integration tests.

All tasks are created with URLs pointing at the AiohttpMocker endpoint:
    http://mock/<slug>/
so AiohttpMocker can route them by slug without needing a real server.
"""

from workflow_orchestrator.conf import get_organization_model, get_credential_model
from workflow_orchestrator.models import Task, Workflow, Schedule, Attribute, TaskAttribute, TaskContinuation


def make_org(name='test-org', **kwargs):
    """Create a minimal Organization using the configured model."""
    Organization = get_organization_model()
    return Organization.objects.create(name=name, **kwargs)


def make_credential(name='test-credential', **kwargs):
    """Create a minimal Credential using the configured model."""
    Credential = get_credential_model()
    kwargs.setdefault('data', {'access_token': 'test-token'})
    return Credential.objects.create(name=name, **kwargs)


def make_schedule(cron='0 * * * *'):
    obj, _ = Schedule.objects.get_or_create(cron_expression=cron)
    return obj


def make_task(slug, method='GET', url=None, timeout=5, **task_fields):
    # Intercept legacy fields for test fixture backward compatibility

    credential_refresh = task_fields.pop('credential_refresh', False)
    retry_config = task_fields.pop('retry_config', {})
    continuation_pipeline = task_fields.pop('continuation_pipeline', [])
    max_iterations = task_fields.pop('max_iterations', 1000)
    retry_rerun_pipeline = task_fields.pop('retry_rerun_pipeline', True)
    fanout_path = task_fields.pop('fanout_path', None)

    task, _ = Task.objects.update_or_create(
        slug=slug,
        defaults={
            'name': slug.replace('_', ' ').title(),
            'max_iterations': max_iterations,
            **task_fields,
        },
    )
    _set_attr(task, 'method', method)
    _set_attr(task, 'url', url or f'http://mock/{slug}/')
    _set_attr(task, 'timeout', timeout)
    if fanout_path:
        _set_attr(task, 'fanout_path', fanout_path)

    if credential_refresh:
        refresh_task, _ = Task.objects.get_or_create(
            slug="global_token_refresh",
            defaults={
                "name": "Global Token Refresh",
                "task_type": Task.TASK_TYPE_HTTP,
            }
        )
        _set_attr(refresh_task, "url", "http://mock/global_token_refresh/")
        _set_attr(refresh_task, "method", "POST")

        max_retries = retry_config.get('max_retries', 3)
        condition = f"{{{{ result.http_status is defined and result.http_status == 401 and (task_params._retry_count | int) < {max_retries} }}}}"

        TaskContinuation.objects.get_or_create(
            primary_task=task,
            continuation_task=refresh_task,
            defaults={
                "condition": condition,
                "priority": 1
            }
        )

    for step in continuation_pipeline:
        cont_type = step.get('type')
        cont_func = step.get('function')

        if cont_type == 'pagination':
            if cont_func == 'pagination.next_page_by_cursor':
                cursor_task, _ = Task.objects.get_or_create(
                    slug="global_cursor_pagination",
                    defaults={
                        "name": "Global Cursor Pagination",
                        "task_type": Task.TASK_TYPE_MUTATION,
                    }
                )
                _set_attr(cursor_task, "mutations", {
                    "_iteration": "{{ (task_params._iteration | int) + 1 }}",
                    "params": {
                        "cursor": "{{ result.response_json.next_cursor }}"
                    }
                })
                TaskContinuation.objects.get_or_create(
                    primary_task=task,
                    continuation_task=cursor_task,
                    defaults={
                        "condition": "{{ result.response_json.next_cursor is defined and result.response_json.next_cursor is not none and result.response_json.next_cursor != '' }}",
                        "priority": 10
                    }
                )

            elif cont_func == 'pagination.next_page_by_page_number':
                page_num_task, _ = Task.objects.get_or_create(
                    slug="global_page_number_pagination",
                    defaults={
                        "name": "Global Page Number Pagination",
                        "task_type": Task.TASK_TYPE_MUTATION,
                    }
                )
                _set_attr(page_num_task, "mutations", {
                    "_iteration": "{{ (task_params._iteration | int) + 1 }}",
                    "params": {
                        "page": "{{ (task_params._iteration | int) + 2 }}"
                    }
                })
                TaskContinuation.objects.get_or_create(
                    primary_task=task,
                    continuation_task=page_num_task,
                    defaults={
                        "condition": '{{ result.response_json["items"] is defined and result.response_json["items"] | length > 0 }}',
                        "priority": 10
                    }
                )

            elif cont_func == 'pagination.next_page_by_offset':
                # Use data-based pagination for cin7 since it returns {'data': [...]}, and items-based for others
                if 'cin7' in slug.lower():
                    offset_task, _ = Task.objects.get_or_create(
                        slug="global_offset_pagination_data",
                        defaults={
                            "name": "Global Offset Pagination (Data)",
                            "task_type": Task.TASK_TYPE_MUTATION,
                        }
                    )
                    _set_attr(offset_task, "mutations", {
                        "_iteration": "{{ (task_params._iteration | int) + 1 }}",
                        "params": {
                            "offset": "{{ (task_params.params.offset | default(0) | int) + (result.response_json[\"data\"] | length) }}"
                        }
                    })
                    # Pagination stops if the returned page size is less than 100 (which is the test suite page size)
                    TaskContinuation.objects.get_or_create(
                        primary_task=task,
                        continuation_task=offset_task,
                        defaults={
                            "condition": '{{ result.response_json["data"] is defined and result.response_json["data"] | length == 100 }}',
                            "priority": 10
                        }
                    )
                else:
                    offset_task, _ = Task.objects.get_or_create(
                        slug="global_offset_pagination_items",
                        defaults={
                            "name": "Global Offset Pagination (Items)",
                            "task_type": Task.TASK_TYPE_MUTATION,
                        }
                    )
                    _set_attr(offset_task, "mutations", {
                        "_iteration": "{{ (task_params._iteration | int) + 1 }}",
                        "params": {
                            "offset": "{{ (task_params.params.offset | default(0) | int) + (result.response_json[\"items\"] | length) }}"
                        }
                    })
                    TaskContinuation.objects.get_or_create(
                        primary_task=task,
                        continuation_task=offset_task,
                        defaults={
                            "condition": '{{ result.response_json["items"] is defined and result.response_json["items"] | length > 0 }}',
                            "priority": 10
                        }
                    )

    return task

def add_retry_continuation(task, strategy='exponential', **retry_config):
    if strategy == 'exponential':
        backoff_task, _ = Task.objects.get_or_create(
            slug="global_exponential_backoff",
            defaults={
                "name": "Global Exponential Backoff",
                "task_type": Task.TASK_TYPE_MUTATION,
            }
        )
        _set_attr(backoff_task, "mutations", {
            "_retry_count": "{{ (task_params._retry_count | int) + 1 }}",
            "_delay_seconds": "{{ 2 ** (task_params._retry_count | int) }}"
        })
        
        status_codes = retry_config.get('status_codes', [429, 500, 502, 503, 504])
        max_retries = retry_config.get('max_retries', 3)
        
        TaskContinuation.objects.get_or_create(
            primary_task=task,
            continuation_task=backoff_task,
            defaults={
                "condition": f"{{{{ ((result.http_status is defined and result.http_status in {status_codes}) or result.status in ['timeout', 'exception']) and (task_params._retry_count | int) < {max_retries} }}}}",
                "priority": 100
            }
        )

    elif strategy == 'fixed_delay':
        fixed_task, _ = Task.objects.get_or_create(
            slug="global_fixed_delay",
            defaults={
                "name": "Global Fixed Delay",
                "task_type": Task.TASK_TYPE_MUTATION,
            }
        )
        base_delay = retry_config.get('base_delay', 30)
        _set_attr(fixed_task, "mutations", {
            "_retry_count": "{{ (task_params._retry_count | int) + 1 }}",
            "_delay_seconds": f"{{{{ {base_delay} }}}}"
        })
        
        status_codes = retry_config.get('status_codes', [429, 500, 502, 503, 504])
        max_retries = retry_config.get('max_retries', 3)

        TaskContinuation.objects.get_or_create(
            primary_task=task,
            continuation_task=fixed_task,
            defaults={
                "condition": f"{{{{ ((result.http_status is defined and result.http_status in {status_codes}) or result.status in ['timeout', 'exception']) and (task_params._retry_count | int) < {max_retries} }}}}",
                "priority": 100
            }
        )


def make_workflow(task, org=None, schedule=None, **wf_fields):
    """Create a Workflow for the given org and task. org may be None for unit tests."""
    return Workflow.objects.create(
        task=task,
        organization=org,
        schedule=schedule,
        workflow_type=wf_fields.pop('workflow_type', Workflow.TYPE_SYNC),
        **wf_fields,
    )


def link_credential(workflow, credential, priority=1):
    """Attach a credential to a workflow via WorkflowCredential."""
    from workflow_orchestrator.models import WorkflowCredential
    return WorkflowCredential.objects.get_or_create(
        workflow=workflow,
        credential=credential,
        defaults={'priority': priority, 'is_active': True},
    )[0]


def link(parent_wf, child_wf):
    """Make child_wf depend on parent_wf (child runs after parent succeeds)."""
    child_wf.mapped_by.add(parent_wf)


def _set_attr(task, name, value):
    attr, _ = Attribute.objects.get_or_create(name=name, value=value)
    TaskAttribute.objects.get_or_create(task=task, attribute=attr)
