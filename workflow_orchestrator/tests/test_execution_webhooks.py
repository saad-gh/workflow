"""
Tests for webhook-triggered workflows.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder

class WebhookExecutionTest(NewArchitectureTestCase):
    def test_webhook_payload_injected(self):
        task = self._create_task(
            name='Webhook Task',
            slug='webhook-task',
            attributes={
                'url': 'http://mock/webhook-task/{{ payload.invoice_id }}'
            }
        )
        ScenarioBuilder.success(task, {'status': 'ok'})
        workflow = self._create_workflow(task)

        webhook_payload = {'invoice_id': 'inv-123'}

        self.run_workflow_sync(
            self.organization.id,
            workflow_id=workflow.id,
            webhook_payload=webhook_payload
        )

        self.assertCallUrl('webhook-task', 'http://mock/webhook-task/inv-123')
