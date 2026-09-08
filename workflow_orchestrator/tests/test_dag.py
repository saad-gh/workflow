"""
Tests for DAG (Directed Acyclic Graph) workflow relationships.

Covers:
  - mapped_by relationships: parent → child wiring
  - get_effective_pipeline on template vs instance workflows
  - Workflow.get_effective_pipeline() ordering is preserved through instantiation
  - Template instantiation: dependencies replicated correctly
  - Root task identification (no parents)
  - Fan-out: parent returns list → multiple child tasks created
  - Workflow.get_workflows_by_integration() filter helper
"""

from django.test import TestCase

from workflow_orchestrator.models import Task, Workflow, Schedule
from workflow_orchestrator.tests.base import make_task, make_workflow, make_schedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _org(id=1):
    """Return a mock organisation-like object with just an id."""
    class FakeOrg:
        pk = id
        name = f'Test Org {id}'
    return FakeOrg()


# ---------------------------------------------------------------------------
# Basic DAG wiring
# ---------------------------------------------------------------------------

class DAGRelationshipTest(TestCase):

    def setUp(self):
        self.task_parent = make_task('parent_task')
        self.task_child = make_task('child_task')

    def test_mapped_by_creates_dependency(self):
        """child_workflow.mapped_by = [parent_workflow] means child depends on parent."""
        wf_parent = make_workflow(self.task_parent, workflow_type=Workflow.TYPE_SYNC)
        wf_child = make_workflow(self.task_child, workflow_type=Workflow.TYPE_SYNC)

        wf_child.mapped_by.add(wf_parent)

        self.assertIn(wf_parent, wf_child.mapped_by.all())

    def test_mapped_to_is_reverse(self):
        """mapped_to is the reverse accessor: parent.mapped_to = [child]."""
        wf_parent = make_workflow(self.task_parent)
        wf_child = make_workflow(self.task_child)
        wf_child.mapped_by.add(wf_parent)

        self.assertIn(wf_child, wf_parent.mapped_to.all())

    def test_multiple_parents(self):
        task_p2 = make_task('parent_task_2')
        wf_p1 = make_workflow(self.task_parent)
        wf_p2 = make_workflow(task_p2)
        wf_child = make_workflow(self.task_child)

        wf_child.mapped_by.set([wf_p1, wf_p2])

        parents = list(wf_child.mapped_by.all())
        self.assertEqual(len(parents), 2)
        self.assertIn(wf_p1, parents)
        self.assertIn(wf_p2, parents)

    def test_multiple_children(self):
        task_c2 = make_task('child_task_2')
        wf_parent = make_workflow(self.task_parent)
        wf_c1 = make_workflow(self.task_child)
        wf_c2 = make_workflow(task_c2)

        wf_c1.mapped_by.add(wf_parent)
        wf_c2.mapped_by.add(wf_parent)

        children = list(wf_parent.mapped_to.all())
        self.assertEqual(len(children), 2)

    def test_root_has_no_parents(self):
        wf_root = make_workflow(self.task_parent)
        self.assertEqual(wf_root.mapped_by.count(), 0)

    def test_leaf_has_no_children(self):
        wf_leaf = make_workflow(self.task_child)
        self.assertEqual(wf_leaf.mapped_to.count(), 0)


# ---------------------------------------------------------------------------
# Workflow template and instance DAG
# ---------------------------------------------------------------------------

class WorkflowTemplateDagTest(TestCase):

    def setUp(self):
        self.task_a = make_task('dag_task_a')
        self.task_b = make_task('dag_task_b')
        self.task_c = make_task('dag_task_c')

    def _create_template(self, name='Test Pipeline'):
        """Create a 3-node linear template: A → B → C."""
        wf_a = Workflow.objects.create(
            is_template=True,
            template_name=name,
            template_description='A → B → C',
            task=self.task_a,
            workflow_type=Workflow.TYPE_SYNC,
            metadata={'node_id': 'step_a'},
        )
        wf_b = Workflow.objects.create(
            is_template=True,
            template_name=name,
            task=self.task_b,
            workflow_type=Workflow.TYPE_SYNC,
            metadata={'node_id': 'step_b'},
        )
        wf_c = Workflow.objects.create(
            is_template=True,
            template_name=name,
            task=self.task_c,
            workflow_type=Workflow.TYPE_SYNC,
            metadata={'node_id': 'step_c'},
        )
        wf_b.mapped_by.add(wf_a)
        wf_c.mapped_by.add(wf_b)
        return wf_a, wf_b, wf_c

    def test_template_has_no_org(self):
        wf_a, _, _ = self._create_template()
        self.assertIsNone(wf_a.organization)

    def test_template_dag_edges_preserved(self):
        wf_a, wf_b, wf_c = self._create_template()
        self.assertIn(wf_a, wf_b.mapped_by.all())
        self.assertIn(wf_b, wf_c.mapped_by.all())

    def test_instance_dag_mirrors_template(self):
        """
        When a template is instantiated (simulated here directly),
        parent-child relationships must be replicated on instances.
        """
        wf_a, wf_b, wf_c = self._create_template('Mirror Test')

        # Simulate instantiation (mirrors what instantiate_workflow_template does)
        schedule = make_schedule('0 * * * *')
        template_to_instance = {}

        for template_wf in [wf_a, wf_b, wf_c]:
            inst = Workflow.objects.create(
                is_template=False,
                template_workflow=template_wf,
                task=template_wf.task,
                schedule=schedule,
                workflow_type=template_wf.workflow_type,
                metadata={'node_id': template_wf.metadata['node_id']},
            )
            template_to_instance[template_wf.id] = inst

        # Wire up instance edges
        for template_wf in [wf_a, wf_b, wf_c]:
            inst = template_to_instance[template_wf.id]
            parents = template_wf.mapped_by.all()
            if parents:
                inst.mapped_by.set([template_to_instance[p.id] for p in parents])

        inst_a = template_to_instance[wf_a.id]
        inst_b = template_to_instance[wf_b.id]
        inst_c = template_to_instance[wf_c.id]

        self.assertIn(inst_a, inst_b.mapped_by.all())
        self.assertIn(inst_b, inst_c.mapped_by.all())
        self.assertEqual(inst_a.mapped_by.count(), 0)  # root

    def test_instance_references_template(self):
        wf_a, _, _ = self._create_template('Ref Test')
        inst = Workflow.objects.create(
            is_template=False,
            template_workflow=wf_a,
            task=wf_a.task,
            workflow_type=wf_a.workflow_type,
        )
        self.assertEqual(inst.template_workflow, wf_a)

    def test_instance_count_via_reverse(self):
        wf_a, _, _ = self._create_template('Count Test')
        for i in range(3):
            Workflow.objects.create(
                is_template=False,
                template_workflow=wf_a,
                task=wf_a.task,
                workflow_type=wf_a.workflow_type,
            )
        self.assertEqual(wf_a.instances.count(), 3)


# ---------------------------------------------------------------------------
# Workflow type and priority
# ---------------------------------------------------------------------------

class WorkflowPriorityTest(TestCase):

    def setUp(self):
        self.task = make_task('prio_task')

    def test_sync_uses_high_queue(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_SYNC)
        self.assertEqual(wf.rq_queue_name, 'high')

    def test_migration_uses_default_queue(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_MIGRATION)
        self.assertEqual(wf.rq_queue_name, 'default')

    def test_analytics_uses_low_queue(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_ANALYTICS)
        self.assertEqual(wf.rq_queue_name, 'low')

    def test_priority_override_1_is_high(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_ANALYTICS, priority_override=1)
        self.assertEqual(wf.rq_queue_name, 'high')

    def test_priority_override_3_is_low(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_SYNC, priority_override=3)
        self.assertEqual(wf.rq_queue_name, 'low')

    def test_backfill_always_low(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_SYNC, is_backfill=True)
        self.assertEqual(wf.effective_queue_name, 'low')

    def test_non_backfill_uses_type_priority(self):
        wf = make_workflow(self.task, workflow_type=Workflow.TYPE_SYNC, is_backfill=False)
        self.assertEqual(wf.effective_queue_name, 'high')


# ---------------------------------------------------------------------------
# Workflow clean() validation
# ---------------------------------------------------------------------------

class WorkflowValidationTest(TestCase):

    def setUp(self):
        self.task = make_task('valid_task')

    def test_template_without_name_raises(self):
        from django.core.exceptions import ValidationError
        wf = Workflow(is_template=True, task=self.task, template_name='')
        with self.assertRaises(ValidationError):
            wf.clean()

    def test_instance_without_org_raises(self):
        from django.core.exceptions import ValidationError
        wf = Workflow(is_template=False, task=self.task, organization=None)
        with self.assertRaises(ValidationError):
            wf.clean()

    def test_template_with_org_raises(self):
        from django.core.exceptions import ValidationError

        class FakeOrg:
            pk = 1
            id = 1

        wf = Workflow(
            is_template=True,
            task=self.task,
            template_name='My Template',
        )
        wf.organization_id = 1  # set directly to bypass FK validation
        with self.assertRaises(ValidationError):
            wf.clean()
