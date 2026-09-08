"""
Tests for pagination logic using TaskContinuations.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import TaskContinuation

class CursorPaginationRunTest(NewArchitectureTestCase):
    def test_cursor_pagination(self):
        list_task = self._create_task(
            name='List Items',
            slug='list-items',
            attributes={
                'url': 'http://mock/list-items/',
                'params': {'cursor': '{{ task_params.cursor|default(None) }}'}
            }
        )
        
        # We need two scenarios for the paginated responses
        (ScenarioBuilder(list_task, 'page_1')
         .match()
         .priority(0)
         .respond(200, {'items': [1, 2], 'next_cursor': 'abc'})
         .save())

        (ScenarioBuilder(list_task, 'page_2')
         .match(cursor='abc')
         .priority(1)
         .respond(200, {'items': [3, 4], 'next_cursor': None})
         .save())

        mutation_task = self._create_task(
            name='Update Cursor',
            slug='update-cursor',
            task_type='mutation',
            attributes={
                'mutations': {
                    'cursor': '{{ result.response_json.next_cursor }}'
                }
            }
        )

        TaskContinuation.objects.create(
            primary_task=list_task,
            continuation_task=mutation_task,
            priority=1,
            condition='{{ result.response_json.next_cursor is not none }}'
        )
        
        TaskContinuation.objects.create(
            primary_task=mutation_task,
            continuation_task=list_task,
            priority=1,
            condition='true'
        )

        self._create_workflow(list_task)
        # mutation_task doesn't need its own workflow, it's called as a continuation

        self.run_workflow_sync(self.organization.id)

        self.assertCallCount('list-items', 2)
