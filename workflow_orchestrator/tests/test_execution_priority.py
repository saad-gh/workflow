"""
Tests for priority, preemption, and credential pooling.
"""
import json
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import Workflow, Task
from asgiref.sync import async_to_sync, sync_to_async

class PreemptionRunTest(NewArchitectureTestCase):
    def test_high_priority_preemption(self):
        task_low = self._create_task('Low Priority Task', 'low-priority-task', attributes={'url': 'http://mock/low-priority-task/'})
        task_high = self._create_task('High Priority Task', 'high-priority-task', attributes={'url': 'http://mock/high-priority-task/'})

        wf_low = self._create_workflow(task_low, priority_override=3)
        wf_high = self._create_workflow(task_high, priority_override=1)

        ScenarioBuilder.success(task_low, {'status': 'ok'})
        ScenarioBuilder.success(task_high, {'status': 'ok'})

        # Manually lock the credential with a low-priority task
        lock_val = {
            "task_id": f"fictitious_run:{wf_low.id}",
            "priority": 3,
            "run_id": "fictitious_run",
            "task": {"task_id": f"fictitious_run:{wf_low.id}"}
        }
        async_to_sync(self._fake_redis.set)(f"cred_lock:{self.credential.id}", json.dumps(lock_val))

        # Now, run the high-priority workflow. It should preempt the lock.
        self.run_workflow_sync(self.organization.id, workflow_id=wf_high.id)

        self.assertCallCount('high-priority-task', 1)
        self.assertEvent("NODE_COMPLETED:high-priority-task")

        # Verify that the low priority lock was preempted and released when high priority completed
        lock_raw = async_to_sync(self._fake_redis.get)(f"cred_lock:{self.credential.id}")
        self.assertIsNone(lock_raw)
