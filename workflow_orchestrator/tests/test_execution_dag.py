"""
Test DAG execution (fan-out, etc.)
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder

class FanOutDagRunTest(NewArchitectureTestCase):
    def test_fan_out(self):
        list_task = self._create_task(
            name='List Users',
            slug='list-users',
            attributes={
                'url': 'http://mock/list-users/',
                'method': 'GET',
                'fanout_path': '' # The whole response is a list
            },
        )
        ScenarioBuilder.success(list_task, [{'id': 1}, {'id': 2}])

        detail_task = self._create_task(
            name='Get User Details',
            slug='get-user-details',
            attributes={
                'url': 'http://mock/get-user-details/?id={{ item.id }}',
                'method': 'GET',
            },
        )
        ScenarioBuilder.success(detail_task, {'id': 1, 'name': 'Alice'})

        list_workflow = self._create_workflow(list_task)
        detail_workflow = self._create_workflow(detail_task)
        
        # Link them using mapped_by (defines DAG topology)
        detail_workflow.mapped_by.add(list_workflow)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:list-users")
        self.assertEvent("NODE_COMPLETED:list-users")
        self.assertEvent("NODE_STARTED:get-user-details")
        self.assertEvent("NODE_COMPLETED:get-user-details")
        
        self.assertCallCount('list-users', 1)
        self.assertCallCount('get-user-details', 2)
