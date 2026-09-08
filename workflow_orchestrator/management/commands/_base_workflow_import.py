"""
Shared base command for importing declarative workflow JSON into the database.

Consumers inherit from this and set ``json_filename``.
The directory containing the JSON (plus its @FILE templates) is resolved from
the ``TASK_MIGRATION_LEGACY_DIR`` setting, falling back to a relative
``.archive/migration_legacy`` path.
"""
import os
import json
import logging
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from workflow_orchestrator.conf import get_organization_model
from workflow_orchestrator.models import (
    Task, Attribute, TaskAttribute, Workflow, WorkflowContinuation, TaskContinuation
)

logger = logging.getLogger(__name__)


class BaseWorkflowImportCommand(BaseCommand):
    """Base class that imports a legacy workflow from a declarative JSON file."""
    json_filename = None  # subclasses must set this

    def add_arguments(self, parser):
        parser.add_argument('--tenant-id', type=str, help='Tenant ID (tenant_id) to assign workflows to.')
        parser.add_argument('--dry-run', action='store_true', help='Parse JSON without committing.')

    def _resolve_base_dir(self):
        configured = getattr(settings, 'TASK_MIGRATION_LEGACY_DIR', None)
        if configured:
            return os.path.abspath(configured)
        return os.path.abspath(os.path.join('.archive', 'migration_legacy'))

    def resolve_placeholders(self, data, base_dir):
        if isinstance(data, str):
            if data.startswith('@FILE:'):
                rel_path = data[len('@FILE:'):]
                abs_path = os.path.join(base_dir, rel_path)
                if not os.path.exists(abs_path):
                    raise CommandError(f"Referenced file not found: {abs_path}")
                with open(abs_path, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            return data
        elif isinstance(data, dict):
            return {k: self.resolve_placeholders(v, base_dir) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.resolve_placeholders(item, base_dir) for item in data]
        return data

    def handle(self, *args, **options):
        if not self.json_filename:
            raise CommandError(f"{self.__class__.__name__} must define json_filename")

        base_dir = self._resolve_base_dir()
        json_path = os.path.join(base_dir, self.json_filename)

        if not os.path.exists(json_path):
            raise CommandError(f"Migration JSON not found: {json_path}")

        self.stdout.write(self.style.NOTICE(f"Loading migration payload from: {json_path}"))

        with open(json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        resolved_data = self.resolve_placeholders(raw_data, base_dir)

        if options['dry_run']:
            self.stdout.write(self.style.SUCCESS("Dry run successful. Resolved placeholders:"))
            self.stdout.write(json.dumps(resolved_data, indent=2))
            return

        TenantModel = get_organization_model()
        tenant_id = options.get('tenant_id')
        tenant = None

        if tenant_id:
            tenant = TenantModel.objects.filter(tenant_id=tenant_id).first()
            if not tenant:
                raise CommandError(f"Tenant with tenant_id '{tenant_id}' does not exist.")
        else:
            tenant = TenantModel.objects.first()

        with transaction.atomic():
            tasks_by_slug = {}
            tasks_created = 0
            tasks_updated = 0

            for task_def in resolved_data.get('tasks', []):
                slug = task_def['slug']
                name = task_def['name']
                task_type = task_def.get('task_type', Task.TASK_TYPE_HTTP)
                max_iterations = task_def.get('max_iterations', 1000)

                task, created = Task.objects.update_or_create(
                    slug=slug,
                    defaults={
                        'name': name,
                        'task_type': task_type,
                        'max_iterations': max_iterations,
                    }
                )
                if created:
                    tasks_created += 1
                else:
                    tasks_updated += 1

                tasks_by_slug[slug] = task

                for attr_name, attr_val in task_def.get('attributes', {}).items():
                    attr_obj = Attribute.objects.filter(name=attr_name, value=attr_val).first()
                    if not attr_obj:
                        attr_obj = Attribute.objects.create(name=attr_name, value=attr_val)
                    TaskAttribute.objects.get_or_create(task=task, attribute=attr_obj)

            workflows_by_task_slug = {}
            workflows_created = 0

            for wf_def in resolved_data.get('workflows', []):
                task_slug = wf_def['task_slug']
                task = tasks_by_slug.get(task_slug)
                if not task:
                    raise CommandError(f"Task '{task_slug}' not found for workflow.")

                workflow, created = Workflow.objects.get_or_create(
                    organization=tenant,
                    task=task,
                    defaults={'priority_override': wf_def.get('priority_override', 2)}
                )
                if created:
                    workflows_created += 1
                workflows_by_task_slug[task_slug] = workflow

            workflow_links_created = 0
            for link_def in resolved_data.get('workflow_links', []):
                parent_wf = workflows_by_task_slug.get(link_def['parent_task_slug'])
                child_wf = workflows_by_task_slug.get(link_def['child_task_slug'])
                if not parent_wf or not child_wf:
                    raise CommandError(f"Missing workflow for link: {link_def}")
                child_wf.mapped_by.add(parent_wf)
                workflow_links_created += 1

            continuations_created = 0
            for cont_def in resolved_data.get('workflow_continuations', []):
                wf_slug = cont_def['workflow_task_slug']
                gk_slug = cont_def.get('gatekeeper_task_slug', wf_slug)
                wf = workflows_by_task_slug.get(wf_slug)
                gk_task = tasks_by_slug.get(gk_slug)
                if not wf or not gk_task:
                    raise CommandError(f"Missing workflow/task for gatekeeper: {cont_def}")
                _, created = WorkflowContinuation.objects.update_or_create(
                    workflow=wf, task=gk_task,
                    defaults={'priority': cont_def.get('priority', 1), 'condition': cont_def.get('condition', 'true')}
                )
                if created:
                    continuations_created += 1

            task_continuations_created = 0
            for tcont_def in resolved_data.get('task_continuations', []):
                primary_task = tasks_by_slug.get(tcont_def['primary_task_slug'])
                continuation_task = tasks_by_slug.get(tcont_def['continuation_task_slug'])
                if not primary_task or not continuation_task:
                    raise CommandError(f"Missing task for TaskContinuation: {tcont_def}")
                _, created = TaskContinuation.objects.update_or_create(
                    primary_task=primary_task, continuation_task=continuation_task,
                    defaults={'priority': tcont_def.get('priority', 1), 'condition': tcont_def.get('condition', 'true')}
                )
                if created:
                    task_continuations_created += 1

            self.stdout.write(self.style.SUCCESS(
                f"Successfully imported {self.json_filename}!\n"
                f"- Tasks created: {tasks_created}, updated: {tasks_updated}\n"
                f"- Workflows created: {workflows_created}\n"
                f"- WorkflowContinuations created: {continuations_created}\n"
                f"- TaskContinuations created: {task_continuations_created}"
            ))
