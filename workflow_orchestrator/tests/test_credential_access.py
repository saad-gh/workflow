"""
Unit tests for workflow credential access patterns.
Tests the new behavior where credentials are accessed via WorkflowCredential and respect is_active flag.
"""

from django.test import TestCase
from django.conf import settings

from workflow_orchestrator.models import Workflow, Organization, Credential, WorkflowCredential
from workflow_orchestrator.tests.fixtures import make_org, make_credential, make_task, make_workflow, link_credential


class CredentialAccessTest(TestCase):
    """Test workflow credential resolution and activity status filters."""

    def setUp(self):
        self.org = make_org(name="Test Org")
        self.task = make_task('test_task')
        self.credential1 = make_credential(name='wf-cred-1')
        self.credential2 = make_credential(name='wf-cred-2')
        self.credential3 = make_credential(name='wf-cred-3')

    def test_workflow_credentials_priority_order(self):
        """Workflow credentials are returned in priority order."""
        workflow = make_workflow(self.task, self.org)
        # Link in non-priority order
        link_credential(workflow, self.credential2, priority=2)
        link_credential(workflow, self.credential1, priority=1)

        credentials = workflow.get_credentials()
        credential_ids = [c.id for c in credentials]

        self.assertEqual(len(credentials), 2)
        self.assertEqual(credential_ids, [self.credential1.id, self.credential2.id])

    def test_inactive_credential_excluded(self):
        """Workflow credentials with is_active=False are excluded from get_credentials."""
        workflow = make_workflow(self.task, self.org)
        wc1 = link_credential(workflow, self.credential1, priority=1)
        wc2 = link_credential(workflow, self.credential2, priority=2)

        # Deactivate wc1
        wc1.is_active = False
        wc1.save()

        credentials = workflow.get_credentials()
        credential_ids = [c.id for c in credentials]

        self.assertEqual(len(credentials), 1)
        self.assertEqual(credential_ids, [self.credential2.id])

    def test_primary_credential_skips_inactive(self):
        """get_primary_credential returns the highest-priority active credential, skipping inactive ones."""
        workflow = make_workflow(self.task, self.org)
        wc1 = link_credential(workflow, self.credential1, priority=1)
        wc2 = link_credential(workflow, self.credential2, priority=2)

        # Deactivate wc1 (the primary/highest priority)
        wc1.is_active = False
        wc1.save()

        primary_credential = workflow.get_primary_credential()
        self.assertEqual(primary_credential.id, self.credential2.id)

    def test_no_credentials_empty_queryset(self):
        """Empty queryset when no credentials exist on the workflow."""
        workflow = make_workflow(self.task, self.org)
        credentials = workflow.get_credentials()
        self.assertEqual(len(credentials), 0)
