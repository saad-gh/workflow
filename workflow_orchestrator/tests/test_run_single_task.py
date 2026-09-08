"""
Test a simple, single-task workflow run.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder

class SingleTaskRunTest(NewArchitectureTestCase):
    def test_single_http_task(self):
        task = self._create_task(
            name='Get Posts',
            slug='get-posts',
            attributes={
                'url': 'http://mock/get-posts/',
                'method': 'GET',
            },
        )
        ScenarioBuilder.success(task, {'id': 1, 'title': 'Test Post'})
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:get-posts")
        self.assertEvent("NODE_COMPLETED:get-posts")
        self.assertCallCount('get-posts', 1)
