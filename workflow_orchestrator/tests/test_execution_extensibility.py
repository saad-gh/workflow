"""
Tests for extensibility (PythonExecutor, OrmExecutor).
"""
from .base_integration import NewArchitectureTestCase
from workflow_orchestrator.mock_server import ScenarioBuilder
from workflow_orchestrator.models import Task, Organization

def sample_python_fn(context):
    return {"result": "success", "value": 42}

def dummy_jinja_loader():
    return {'custom_jinja_helper': lambda: 'dynamic_jinja_value'}

from workflow_orchestrator.executors import BaseExecutor

class CustomTestExecutor(BaseExecutor):
    async def execute(self, context, node):
        return {"status": "custom_success", "custom_val": 100}


class ExtensibilityExecutionTest(NewArchitectureTestCase):
    def test_python_executor(self):
        task = self._create_task(
            name='Python Task',
            slug='python-task',
            task_type='python',
            attributes={
                'python_callable': 'workflow_orchestrator.tests.test_execution_extensibility.sample_python_fn'
            }
        )
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:python-task")
        self.assertEvent("NODE_COMPLETED:python-task")

    def test_orm_executor_create(self):
        task = self._create_task(
            name='ORM Create Task',
            slug='orm-create-task',
            task_type='orm',
            attributes={
                'orm_model': 'workflow_orchestrator.Organization',
                'orm_action': 'create',
                'orm_fields': {'name': 'New Org From ORM'}
            }
        )
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:orm-create-task")
        self.assertEvent("NODE_COMPLETED:orm-create-task")
        self.assertTrue(Organization.objects.filter(name='New Org From ORM').exists())

    def test_human_input_executor(self):
        task = self._create_task(
            name='Human Input Task',
            slug='human-input-task',
            task_type='human_input',
            attributes={
                'prompt': 'Please enter code',
                'input_key': 'auth_code'
            }
        )
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:human-input-task")
        self.assertEvent("NODE_COMPLETED:human-input-task")

    def test_checkpoint_executor(self):
        task = self._create_task(
            name='Checkpoint Task',
            slug='checkpoint-task',
            task_type='checkpoint',
            attributes={}
        )
        self._create_workflow(task)

        self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:checkpoint-task")
        self.assertEvent("NODE_COMPLETED:checkpoint-task")

    def test_custom_registered_executor(self):
        task = self._create_task(
            name='Custom Registered Task',
            slug='custom-registered-task',
            task_type='custom_type',
            attributes={}
        )
        self._create_workflow(task)

        with self.settings(TASK_EXECUTORS={'custom_type': 'workflow_orchestrator.tests.test_execution_extensibility.CustomTestExecutor'}):
            self.run_workflow_sync(self.organization.id)

        self.assertEvent("NODE_STARTED:custom-registered-task")
        self.assertEvent("NODE_COMPLETED:custom-registered-task")

    def test_jinja_helpers_loader(self):
        from workflow_orchestrator.jinja_helpers import get_jinja_env
        with self.settings(TASK_JINJA_HELPERS_LOADER='workflow_orchestrator.tests.test_execution_extensibility.dummy_jinja_loader'):
            env = get_jinja_env()
            template = env.from_string("{{ custom_jinja_helper() }}")
            rendered = template.render()
            self.assertEqual(rendered, "dynamic_jinja_value")
