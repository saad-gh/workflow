import os

from django.conf import settings
from django.urls import path
from . import views

app_name = 'task'

urlpatterns = [
    path('orchestrator/start-run/', views.start_run_view, name='start_run'),
    path('workflow/start/', views.start_workflow_run_rq, name='start_workflow_rq'),
    path('workflow/job-status/', views.get_job_status, name='get_job_status'),
    path('checkpoints/list/', views.list_checkpoints_view, name='list_checkpoints'),
    path('checkpoints/resume/', views.resume_checkpoints_view, name='resume_checkpoints'),
]

# Mock server endpoint — only available in development and test environments.
# Never exposed in production (DEBUG=False, TESTING unset).
if settings.DEBUG or getattr(settings, 'TESTING', False):
    from .mock_server import mock_response_view
    urlpatterns += [
        path('mock/<slug:task_slug>/', mock_response_view, name='mock_response'),
    ]
