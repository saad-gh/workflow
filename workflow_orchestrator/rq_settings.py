"""
Django-RQ configuration settings.

Add this to your Django project's settings.py:

# RQ (Redis Queue) Configuration
from workflow_orchestrator.rq_settings import RQ_QUEUES

"""
import os

# Redis connection configuration (using existing MemoryStore)
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_AUTH")

# Build Redis URL
if REDIS_PASSWORD:
    REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# RQ Queue Configuration
RQ_QUEUES = {
    'default': {
        'HOST': REDIS_HOST,
        'PORT': REDIS_PORT,
        'DB': REDIS_DB,
        'PASSWORD': REDIS_PASSWORD,
        'DEFAULT_TIMEOUT': '30m',  # 30 minutes default timeout
    },
    'high': {
        'HOST': REDIS_HOST,
        'PORT': REDIS_PORT,
        'DB': REDIS_DB,
        'PASSWORD': REDIS_PASSWORD,
        'DEFAULT_TIMEOUT': '10m',
    },
    'low': {
        'HOST': REDIS_HOST,
        'PORT': REDIS_PORT,
        'DB': REDIS_DB,
        'PASSWORD': REDIS_PASSWORD,
        'DEFAULT_TIMEOUT': '60m',
    },
}

# RQ Settings
RQ_SHOW_ADMIN_LINK = True  # Show RQ admin in Django admin

# RQ Exception Handlers
RQ_EXCEPTION_HANDLERS = [
    'workflow_orchestrator.rq_handlers.log_exception_handler',
]
