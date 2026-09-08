"""
Base test cases for the new OOP architecture.
"""
import asyncio
from unittest.mock import patch
from django.test import TransactionTestCase
from asgiref.sync import sync_to_async, async_to_sync

from workflow_orchestrator.models import (
    Organization, Service, Resource, Task, Workflow, Credential, Attribute, TaskAttribute, WorkflowCredential
)
from workflow_orchestrator.traverser import start_workflow_run
from workflow_orchestrator.telemetry import TelemetryLogger
from workflow_orchestrator.mock_server import AiohttpMocker

class MockTelemetryLogger(TelemetryLogger):
    instances = []
    def __init__(self, run_id: str, org_id: int):
        super().__init__(run_id, org_id)
        self.events = []
        MockTelemetryLogger.instances.append(self)

    def log(self, event_type: str, task_slug: str, **kwargs):
        reason = kwargs.get('reason')
        if reason:
            self.events.append(f"{event_type}:{task_slug}:{reason}")
        else:
            self.events.append(f"{event_type}:{task_slug}")
        super().log(event_type, task_slug, **kwargs)


class NewArchitectureTestCase(TransactionTestCase):
    def setUp(self):
        super().setUp()
        from workflow_orchestrator.models import MockTaskResponse
        MockTaskResponse.objects.all().delete()

        self.organization = Organization.objects.create(name="Test Org")
        self.service = Service.objects.create(name="Test Service")
        self.resource = Resource.objects.create(name="Test Resource", service=self.service)
        self.credential = Credential.objects.create(name="Test Credential", service_id=self.service.id)
        self.mocker = AiohttpMocker()
        self.logger = None

        import fakeredis.aioredis
        import workflow_orchestrator.executors
        self._fake_redis = fakeredis.aioredis.FakeRedis()
        workflow_orchestrator.executors._ASYNC_RDB = self._fake_redis
        workflow_orchestrator.executors._HTTP_SESSION = None

    def tearDown(self):
        import workflow_orchestrator.executors
        workflow_orchestrator.executors._ASYNC_RDB = None
        workflow_orchestrator.executors._HTTP_SESSION = None
        super().tearDown()

    @sync_to_async
    def _create_task_async(self, name, slug, task_type='http', attributes=None, **kwargs):
        return self._create_task(name, slug, task_type, attributes, **kwargs)

    @sync_to_async
    def _create_workflow_async(self, task, **kwargs):
        return self._create_workflow(task, **kwargs)

    def _create_task(self, name, slug, task_type='http', attributes=None, **kwargs):
        task = Task.objects.create(name=name, slug=slug, task_type=task_type, resource=self.resource, **kwargs)
        if attributes:
            for key, value in attributes.items():
                attr, _ = Attribute.objects.get_or_create(name=key, value=value)
                TaskAttribute.objects.create(task=task, attribute=attr)
        return task

    def _create_workflow(self, task, **kwargs):
        workflow = Workflow.objects.create(task=task, organization=self.organization, **kwargs)
        WorkflowCredential.objects.create(workflow=workflow, credential=self.credential)
        return workflow

    def run_workflow_sync(self, org_id, schedule_id=None, workflow_id=None, webhook_payload=None):
        """Full run, with Telemetry and HTTP mocking, wrapped for synchronous tests."""
        async def _run():
            with patch('workflow_orchestrator.traverser.TelemetryLogger', new=MockTelemetryLogger):
                await start_workflow_run(org_id, schedule_id=schedule_id, workflow_id=workflow_id, webhook_payload=webhook_payload)
                if MockTelemetryLogger.instances:
                    self.logger = MockTelemetryLogger.instances[0]
        
        MockTelemetryLogger.instances = [] # Reset instances
        # Enter the mocker context BEFORE async_to_sync so _load_scenarios runs synchronously
        with self.mocker:
            async_to_sync(_run)()

    def assertEvent(self, event_string):
        if not self.logger:
            self.fail("Logger not available. Make sure to capture it from the patched constructor.")
        self.assertIn(event_string, self.logger.events)

    def assertCallCount(self, task_slug, expected):
        actual = self.mocker.call_count(task_slug)
        self.assertEqual(
            actual, expected,
            f"Expected {expected} HTTP calls to '{task_slug}', got {actual}. "
            f"Statuses: {self.mocker.statuses(task_slug)}",
        )

    def assertCallUrl(self, task_slug, expected_url):
        actual_url = self.mocker.last_url(task_slug)
        self.assertEqual(actual_url, expected_url)
