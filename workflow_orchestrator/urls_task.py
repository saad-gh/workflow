"""
Minimal URL configuration for the task service standalone deployment.
Only mounts the task app endpoints — nothing from the wider platform.
"""

from django.urls import path, include
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('task/', include('workflow_orchestrator.urls')),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
