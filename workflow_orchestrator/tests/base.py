"""
Base test case for workflow orchestration unit tests (no HTTP, no Redis).
"""

import asyncio

from django.test import TestCase

from workflow_orchestrator.models import Task, Workflow, Schedule, Attribute, TaskAttribute
from workflow_orchestrator.mock_server import AiohttpMocker, ScenarioBuilder, reset_view_call_counts
from workflow_orchestrator.tests.fixtures import make_task, make_workflow, make_schedule


class WorkflowTestCase(TestCase):
    """
    Base class for unit tests of continuation functions, deduplication,
    pipeline injection, and mock server mechanics.

    Provides a fresh AiohttpMocker each test and assertion helpers that
    match the integration base class API so tests read consistently.
    """

    sb = ScenarioBuilder

    def setUp(self):
        super().setUp()
        reset_view_call_counts()
        self.mocker = AiohttpMocker()

    def tearDown(self):
        reset_view_call_counts()
        super().tearDown()

    def run_async(self, coro):
        return asyncio.run(coro)

    def assertCallCount(self, task_slug: str, expected: int, mocker: AiohttpMocker = None):
        m = mocker or self.mocker
        actual = m.call_count(task_slug)
        self.assertEqual(
            actual, expected,
            f"Expected {expected} calls to '{task_slug}', got {actual}. "
            f"Statuses: {m.statuses(task_slug)}"
        )

    def assertLastStatus(self, task_slug: str, expected: int, mocker: AiohttpMocker = None):
        m = mocker or self.mocker
        actual = m.last_status(task_slug)
        self.assertEqual(
            actual, expected,
            f"Expected last status {expected} for '{task_slug}', got {actual}. "
            f"All statuses: {m.statuses(task_slug)}"
        )
