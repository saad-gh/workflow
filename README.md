# workflow-orchestrator

Standalone Django app for orchestrating multi-step workflow execution using a task-based architecture.

## Features

- **Task-based DAG execution** — define tasks, link them into directed acyclic graphs, execute in parallel
- **Continuation pipeline** — cursor/offset/page/date pagination, exponential backoff, rate-limit retry, credential refresh, incremental sync
- **Deduplication** — by ID, composite key, or content hash (Redis-backed)
- **Priority queuing** — high / default / low queues via RQ (Redis Queue)
- **Webhook triggers** — HMAC / Bearer / Basic auth, event filtering, rate limiting
- **Backfill strategies** — full, date-range, optimised with checkpointing
- **Swappable models** — plug in your own Credential, Organization, Resource models via settings

## Installation

```bash
pip install workflow-orchestrator
# with postgres
pip install "workflow-orchestrator[postgres]"
# with GCP cloud run/tasks support
pip install "workflow-orchestrator[gcp]"
```

## Quick start

### 1. Add to `INSTALLED_APPS`

```python
INSTALLED_APPS = [
    ...
    'django_rq',
    'workflow_orchestrator',
]
```

### 2. Configure Redis

```python
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB   = 0
REDIS_AUTH = None

RQ_QUEUES = {
    'high':    {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
    'default': {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
    'low':     {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
}
```

### 3. Include URLs

```python
# urls.py
from django.urls import path, include
urlpatterns = [
    path('workflow/', include('workflow_orchestrator.urls')),
]
```

### 4. Migrate

```bash
python manage.py migrate workflow_orchestrator
```

### 5. Start an RQ worker

```bash
python manage.py rqworker high default low
```

## Swappable models

By default the app ships with built-in `Credential`, `Organization`, and `Resource` models.
Point these at your own models via settings:

```python
TASK_CREDENTIAL_MODEL  = 'myapp.Credential'   # must have a JSONField for auth data
TASK_ORGANIZATION_MODEL = 'myapp.Organization' # must be in INSTALLED_APPS
TASK_RESOURCE_MODEL    = 'myapp.Resource'      # must have url and limits fields

# Field names on your models (defaults shown):
TASK_CREDENTIAL_DATA_FIELD          = 'data'
TASK_ORGANIZATION_EXTERNAL_ID_FIELD = 'external_id'
TASK_ORGANIZATION_CREDENTIALS_FIELD = 'credentials'  # field for accessing credentials from organization
TASK_RESOURCE_LIMITS_FIELD          = 'limits'
```

## Running tests

```bash
pip install "workflow-orchestrator[test]"
python manage.py test workflow_orchestrator.tests --settings=tests.settings
```
