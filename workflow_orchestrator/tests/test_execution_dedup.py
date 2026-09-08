"""
Tests for response item deduplication during workflow execution.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder

class DeduplicationExecutionTest(NewArchitectureTestCase):
    def test_by_id_deduplication_filters_items_before_fan_out(self):
        """
        Parent task returns 3 items (2 unique). Only 2 child tasks should spawn.
        """
        list_task = self._create_task(
            name='List Duplicates',
            slug='list-duplicates',
            attributes={
                'url': 'http://mock/list-duplicates/',
                'fanout_path': 'items'
            },
            deduplication_strategy='by_id',
            deduplication_config={'key_fields': ['id']}
        )
        ScenarioBuilder.success(list_task, {'items': [{'id': 1}, {'id': 2}, {'id': 1}]})

        detail_task = self._create_task(
            name='Process Item',
            slug='process-item',
            attributes={'url': 'http://mock/process-item/{{ item.id }}'}
        )
        ScenarioBuilder.success(detail_task, {'status': 'processed'})

        list_workflow = self._create_workflow(list_task)
        detail_workflow = self._create_workflow(detail_task)
        detail_workflow.mapped_by.add(list_workflow)

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('list-duplicates', 1)
        self.assertCallCount('process-item', 2) # Assert that only 2 of 3 items were processed
