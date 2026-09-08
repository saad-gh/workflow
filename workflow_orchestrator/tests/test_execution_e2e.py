"""
End-to-End tests for workflow execution, cross-workflow links, and data egress logs.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import DataEgressLog

class EndToEndExecutionTest(NewArchitectureTestCase):
    def test_data_egress_logging(self):
        """Verify DataEgressLog records are generated after HTTP task execution."""
        task = self._create_task(
            name='Extract Orders',
            slug='extract-orders',
            attributes={'url': 'http://mock/extract-orders/'}
        )
        ScenarioBuilder.success(task, {'orders': [{'id': 1}, {'id': 2}]})
        workflow = self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('extract-orders', 1)
        
        # Drain egress queue from Redis into DataEgressLog table
        import json
        from asgiref.sync import async_to_sync
        raw = async_to_sync(self._fake_redis.lpop)("egress:db_write_queue")
        self.assertIsNotNone(raw)
        record = json.loads(raw)
        self.assertEqual(record["organization_id"], self.organization.id)
        self.assertGreater(record["total_size_bytes"], 0)

    def test_cross_workflow_dag_continuation(self):
        """
        Verify Workflow A (Extract) -> Workflow B (Load) linked via mapped_by.
        Executing org run executes both workflows sequentially.
        """
        extract_task = self._create_task(
            name='Extract Step',
            slug='extract-step',
            attributes={'url': 'http://mock/extract-step/'}
        )
        ScenarioBuilder.success(extract_task, {'records': [100, 200]})

        load_task = self._create_task(
            name='Load Step',
            slug='load-step',
            attributes={'url': 'http://mock/load-step/'}
        )
        ScenarioBuilder.success(load_task, {'loaded': True})

        wf_extract = self._create_workflow(extract_task)
        wf_load = self._create_workflow(load_task)

        # Link Workflow A -> Task B via mapped_by
        wf_load.mapped_by.add(wf_extract)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:extract-step")
        self.assertEvent("NODE_COMPLETED:extract-step")
        self.assertEvent("NODE_STARTED:load-step")
        self.assertEvent("NODE_COMPLETED:load-step")

        self.assertCallCount('extract-step', 1)
        self.assertCallCount('load-step', 1)

    def test_backfill_queue_routing(self):
        """Verify is_backfill correctly routes effective_queue_name to low."""
        task = self._create_task('Backfill Task', 'backfill-task')
        workflow = self._create_workflow(task, is_backfill=True, priority_override=1)

        self.assertEqual(workflow.effective_queue_name, 'low')
