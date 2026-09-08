"""
Tests for the new OOP architecture.
"""
import asyncio
from unittest.mock import patch
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder

class TestNewArchitecture(NewArchitectureTestCase):
    def test_simple_workflow_run(self):
        task = self._create_task("Test Task", "test-task", attributes={"url": "http://mock/test-task/"})
        ScenarioBuilder.success(task, {"status": "success"})
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:test-task")
        self.assertEvent("NODE_COMPLETED:test-task")
        self.assertCallCount('test-task', 1)
