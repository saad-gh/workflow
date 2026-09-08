# workflow-orchestrator

> **Durable Task-Based Workflow Execution Substrate for Django**  
> Standalone Django application for orchestrating multi-step DAG workflow execution, serving as the execution substrate for `workflow-agent`.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)]()

---

## Features

- **Task-based DAG execution** — define tasks, link them into directed acyclic graphs via `mapped_by`, and execute in parallel.
- **Declarative Continuations** — Jinja2-conditioned self-loops (`TaskContinuation`) for cursor/offset/page pagination, exponential backoff retries, and rate limiting.
- **Pre-Execution Gatekeepers** — `WorkflowContinuation` pre-task evaluation for permission and policy enforcement before task execution.
- **Durable Checkpointing & Resumption** — pause workflows on rate limit, quota exhaustion, or human approval requirements, saving context to `Checkpoint` for seamless API resumption.
- **Deduplication & Idempotency** — Redis-backed deduplication by ID, composite key, or content hash.
- **Priority Queuing** — high / default / low queues via RQ (Redis Queue) with priority overrides per workflow type.
- **Mock Response Infrastructure** — built-in `AiohttpMocker` and `ScenarioBuilder` for offline, 100% deterministic testing and rate-limit retry simulation.
- **Swappable Models** — plug in your own `Credential`, `Organization`, `Resource`, or `Service` models via Django settings.

---

## Installation

```bash
# Direct installation from public GitHub repository:
pip install "workflow-orchestrator @ git+https://github.com/saad-gh/workflow.git"

# With optional dependencies (PostgreSQL support / test fakeredis):
pip install "workflow-orchestrator[postgres,test] @ git+https://github.com/saad-gh/workflow.git"

# Local editable installation for development:
pip install -e /path/to/workflow-orchestrator
```

---

## Quick Start

### 1. Add to `INSTALLED_APPS`

```python
INSTALLED_APPS = [
    ...
    'django_rq',
    'workflow_orchestrator',
]
```

### 2. Configure Redis or Zero-Infra `fakeredis`

```python
# Standard Redis Configuration:
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB   = 0
REDIS_AUTH = None

RQ_QUEUES = {
    'high':    {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
    'default': {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
    'low':     {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB},
}

# Zero-Infrastructure Local Testing / Offline Mode:
# Set USE_FAKEREDIS = True in settings to run fully offline without a Redis daemon.
USE_FAKEREDIS = True
```

### 3. Include URLs

```python
# urls.py
from django.urls import path, include

urlpatterns = [
    path('task/', include('workflow_orchestrator.urls')),
]
```

### 4. Run Migrations

```bash
python manage.py migrate workflow_orchestrator
```

### 5. Start an RQ Worker (Production Mode)

```bash
python manage.py rqworker high default low
```

---

## Swappable Models Configuration

By default, `workflow_orchestrator` ships with built-in `Credential`, `Organization`, `Service`, and `Resource` models.
Point these at your own application models via settings without modifying orchestrator code:

```python
TASK_CREDENTIAL_MODEL  = 'myapp.Credential'   # must have a JSONField for auth data
TASK_ORGANIZATION_MODEL = 'myapp.Organization' # must be in INSTALLED_APPS
TASK_RESOURCE_MODEL    = 'myapp.Resource'      # must have url and limits fields
TASK_SERVICE_MODEL     = 'myapp.Service'

# Custom model field mappings (defaults shown):
TASK_CREDENTIAL_DATA_FIELD          = 'data'
TASK_ORGANIZATION_EXTERNAL_ID_FIELD = 'external_id'
TASK_ORGANIZATION_CREDENTIALS_FIELD = 'credentials'
TASK_RESOURCE_LIMITS_FIELD          = 'limits'
```

---

## Running Tests

The test suite contains 97 deterministic integration and unit tests:

```bash
# Install test dependencies
pip install "workflow-orchestrator[test]"

# Run standalone test suite (requires PostgreSQL on localhost:5433 or configured DB)
python manage.py test workflow_orchestrator.tests --settings=tests.settings
```

---

## License

Distributed under the **Apache License, Version 2.0**. See [`LICENSE`](LICENSE) for details.
