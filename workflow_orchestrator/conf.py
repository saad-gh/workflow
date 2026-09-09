"""
Configuration helpers for the task app's swappable models.

Default models (used when settings are not overridden):
    TASK_CREDENTIAL_MODEL  = 'workflow_orchestrator.Credential'
    TASK_ORGANIZATION_MODEL = 'workflow_orchestrator.Organization'
    TASK_RESOURCE_MODEL    = 'workflow_orchestrator.Resource'

To use external models instead, add to settings.py:

    TASK_CREDENTIAL_MODEL  = '<app_name>.<model_name>'
    TASK_ORGANIZATION_MODEL = '<app_name>.<model_name>'
    TASK_RESOURCE_MODEL    = '<app_name>.<model_name>'
    TASK_ORGANIZATION_EXTERNAL_ID_FIELD = 'customer_id'  # default: 'external_id'
    TASK_CREDENTIAL_DATA_FIELD          = 'data'         # default: 'data'
    TASK_RESOURCE_LIMITS_FIELD          = 'limits'       # default: 'limits'
    TASK_ORGANIZATION_CREDENTIALS_FIELD = 'credentials'  # default: 'credentials'
"""

from django.conf import settings
from django.apps import apps


def get_credential_model():
    model_str = getattr(settings, 'TASK_CREDENTIAL_MODEL', 'workflow_orchestrator.Credential')
    return apps.get_model(model_str)


def get_organization_model():
    model_str = getattr(settings, 'TASK_ORGANIZATION_MODEL', 'workflow_orchestrator.Organization')
    return apps.get_model(model_str)


def get_resource_model():
    model_str = getattr(settings, 'TASK_RESOURCE_MODEL', 'workflow_orchestrator.Resource')
    return apps.get_model(model_str)


def get_service_model():
    model_str = getattr(settings, 'TASK_SERVICE_MODEL', 'workflow_orchestrator.Service')
    return apps.get_model(model_str)


def get_organization_external_id(org) -> str:
    """Return the tenant external-id value from an Organization instance."""
    field = getattr(settings, 'TASK_ORGANIZATION_EXTERNAL_ID_FIELD', 'external_id')
    return str(getattr(org, field, '') or org.pk)


def get_credential_data(credential) -> dict:
    """Return the auth data dict from a Credential instance."""
    field = getattr(settings, 'TASK_CREDENTIAL_DATA_FIELD', 'data')
    return getattr(credential, field, {}) or {}


def get_organization_credentials_field():
    """Return the field name for credentials on Organization model."""
    return getattr(settings, 'TASK_ORGANIZATION_CREDENTIALS_FIELD', 'credentials')


def get_service_oauth_config(service) -> dict:
    """
    Return OAuth2 config for a service.
    Model fields (client_id, client_secret) take precedence over metadata keys.
    """
    meta = service.metadata or {}
    oidc = meta.get('openid-config', {})
    return {
        'client_id':              service.client_id or meta.get('client_id', ''),
        'client_secret':          service.client_secret or meta.get('client_secret', ''),
        'scopes':                 meta.get('scopes', ''),
        'authorization_endpoint': oidc.get('authorization_endpoint', ''),
        'token_endpoint':         oidc.get('token_endpoint', ''),
        'revocation_endpoint':    oidc.get('revocation_endpoint', ''),
    }
