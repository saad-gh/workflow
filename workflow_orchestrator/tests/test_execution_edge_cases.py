"""
Tests for DAG and pagination edge cases.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import Task

class DagEdgeCaseTest(NewArchitectureTestCase):
    def setUp(self):
        super().setUp()
        self.parent_task = self._create_task(
            name='Parent Task',
            slug='parent-task',
            attributes={'url': 'http://mock/parent-task/'}
        )
        self.child_task = self._create_task(
            name='Child Task',
            slug='child-task',
            attributes={'url': 'http://mock/child-task/'}
        )
        self.parent_task_empty = self._create_task(
            name='Parent Task Empty',
            slug='parent-task-empty',
            attributes={'url': 'http://mock/parent-task-empty/', 'fanout_path': 'items'}
        )
        self.child_task_empty = self._create_task(
            name='Child Task Empty',
            slug='child-task-empty',
            attributes={'url': 'http://mock/child-task-empty/'}
        )

    def test_child_not_called_when_parent_fails(self):
        ScenarioBuilder.error(self.parent_task, 500)
        ScenarioBuilder.success(self.child_task, {'status': 'ok'})

        parent_workflow = self._create_workflow(self.parent_task)
        child_workflow = self._create_workflow(self.child_task)
        child_workflow.mapped_by.add(parent_workflow)

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('parent-task', 1)
        self.assertCallCount('child-task', 0)
        self.assertEvent("NODE_STARTED:parent-task")
        # The traverser currently doesn't log a specific "NODE_FAILED" event, but we can see the child was not started.

    def test_no_children_when_parent_returns_empty_list(self):
        ScenarioBuilder.success(self.parent_task_empty, {'items': []})
        ScenarioBuilder.success(self.child_task_empty, {'status': 'ok'})

        parent_workflow = self._create_workflow(self.parent_task_empty)
        child_workflow = self._create_workflow(self.child_task_empty)
        child_workflow.mapped_by.add(parent_workflow)

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('parent-task-empty', 1)
        self.assertCallCount('child-task-empty', 0)
