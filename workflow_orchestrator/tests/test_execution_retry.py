"""
Tests for retry and error handling.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import TaskContinuation

class RetryRunTest(NewArchitectureTestCase):
    def test_500_retried_then_succeeds(self):
        task = self._create_task(
            name='Flaky Task',
            slug='flaky-task',
            attributes={'url': 'http://mock/flaky-task/'}
        )
        (ScenarioBuilder(task, 'flaky')
         .respond(500, {'error': 'internal server error'})
         .respond(200, {'status': 'ok'})
         .save())

        # Create a continuation to retry on 500
        retry_task = self._create_task(name='Retry', slug='retry', task_type='mutation', attributes={'mutations': {}})
        TaskContinuation.objects.create(
            primary_task=task,
            continuation_task=retry_task,
            priority=1,
            condition='{{ result.status_code == 500 }}'
        )
        TaskContinuation.objects.create(
            primary_task=retry_task,
            continuation_task=task,
            priority=1,
            condition='true'
        )

        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('flaky-task', 2)
        self.assertEvent('NODE_STARTED:flaky-task')
        self.assertEvent('NODE_CONTINUATION:flaky-task')
        self.assertEvent('NODE_COMPLETED:flaky-task')
