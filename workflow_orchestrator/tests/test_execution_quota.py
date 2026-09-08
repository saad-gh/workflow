"""
Tests for quota limits and checkpointing.
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import Checkpoint

class QuotaRunTest(NewArchitectureTestCase):
    def test_quota_exceeded(self):
        self.service.limits = {"quota_rules": [{"type": "daily", "limit": 1}]}
        self.service.save()

        task = self._create_task(
            name='Limited Resource',
            slug='limited-resource',
            attributes={'url': 'http://mock/limited-resource/'}
        )
        ScenarioBuilder.success(task, {'data': 'first'})
        
        self._create_workflow(task)

        # First run should succeed
        self.run_workflow_sync(self.organization.id)
        self.assertCallCount('limited-resource', 1)

        # Second run should hit quota limit and create checkpoint
        self.run_workflow_sync(self.organization.id)
        self.assertCallCount('limited-resource', 1) # Should not increment

        # Verify Checkpoint
        checkpoint_count = Checkpoint.objects.count()
        self.assertEqual(checkpoint_count, 1)
        
        checkpoint = Checkpoint.objects.first()
        self.assertEqual(checkpoint.reason, 'quota_exceeded: daily')
