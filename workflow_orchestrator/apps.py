from django.apps import AppConfig
from django.conf import settings


class TaskConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'workflow_orchestrator'

    def ready(self):
        # Django's ``swappable`` mechanism requires these settings to be defined
        # (like AUTH_USER_MODEL). Provide defaults so the library works out of the
        # box; consumers may override them in their own settings.
        defaults = {
            'TASK_CREDENTIAL_MODEL': 'workflow_orchestrator.Credential',
            'TASK_ORGANIZATION_MODEL': 'workflow_orchestrator.Organization',
            'TASK_RESOURCE_MODEL': 'workflow_orchestrator.Resource',
            'TASK_SERVICE_MODEL': 'workflow_orchestrator.Service',
            'TASK_EXECUTORS': {},
            'TASK_JINJA_HELPERS_LOADER': None,
        }
        for key, value in defaults.items():
            if not hasattr(settings, key):
                setattr(settings, key, value)
