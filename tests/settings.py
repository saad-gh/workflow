"""
Minimal settings for running workflow_orchestrator tests standalone.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

SECRET_KEY = 'test-secret-key'
DEBUG = True
TESTING = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'workflow_orchestrator_test',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'HOST': 'localhost',
        'PORT': 5433,
    }
}

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_rq',
    'workflow_orchestrator',
]

ROOT_URLCONF = 'workflow_orchestrator.urls_task'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

MIDDLEWARE = [
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB   = 15
REDIS_AUTH = None
REDIS_TLS  = '0'
REDIS_POOL_SIZE = 2
CONCURRENCY = 1

RQ_QUEUES = {
    'high':    {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB, 'DEFAULT_TIMEOUT': 60},
    'default': {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB, 'DEFAULT_TIMEOUT': 60},
    'low':     {'HOST': REDIS_HOST, 'PORT': REDIS_PORT, 'DB': REDIS_DB, 'DEFAULT_TIMEOUT': 60},
}

MIGRATION_MODULES = {
    'workflow_orchestrator': 'workflow_orchestrator.test_migrations',
}

PASSWORD_HASHERS = ['django.contrib.auth.hashers.MD5PasswordHasher']

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_TZ = True
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

OAUTH_CALLBACK_URL = "https://yourapp.example.com/task/oauth/callback/"
OAUTH_POST_AUTH_REDIRECT_URL = "/"

STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static')

