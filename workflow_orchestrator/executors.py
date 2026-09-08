"""
Defines the Executor classes for handling different task types.
"""
import abc
import aiohttp
import json
import logging
from typing import Dict, Any, Optional, List
from jinja2 import Environment, StrictUndefined, TemplateSyntaxError, UndefinedError
from asgiref.sync import sync_to_async
import redis.asyncio as async_redis

from .execution_context import ExecutionContext
from .quota import CredentialManager, QuotaExceededError, NoAvailableCredentialsError
from .quota_manager import increment_api_usage
from .data_egress_manager import track_data_egress

logger = logging.getLogger(__name__)

_ASYNC_RDB = None

def get_async_redis() -> async_redis.Redis:
    global _ASYNC_RDB
    if _ASYNC_RDB is None:
        from django.conf import settings
        if getattr(settings, 'USE_FAKEREDIS', False):
            import fakeredis.aioredis
            _ASYNC_RDB = fakeredis.aioredis.FakeRedis(decode_responses=False)
        else:
            host = getattr(settings, 'REDIS_HOST', '127.0.0.1')
            port = getattr(settings, 'REDIS_PORT', 6379)
            db = getattr(settings, 'REDIS_DB', 0)
            auth = getattr(settings, 'REDIS_AUTH', None)
            auth_str = f":{auth}@" if auth else ""
            url = f"redis://{auth_str}{host}:{port}/{db}"
            _ASYNC_RDB = async_redis.from_url(url, decode_responses=False)
    return _ASYNC_RDB

def set_async_redis(redis_client):
    global _ASYNC_RDB
    _ASYNC_RDB = redis_client

# A global session for connection pooling
_HTTP_SESSION = None

def get_async_http_session() -> aiohttp.ClientSession:
    """Get or create a persistent aiohttp ClientSession with connection pooling."""
    global _HTTP_SESSION
    if _HTTP_SESSION is None or _HTTP_SESSION.closed:
        connector = aiohttp.TCPConnector(limit=300, limit_per_host=50, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        _HTTP_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _HTTP_SESSION

class BaseExecutor(abc.ABC):
    """Abstract base class for all executors."""

    @abc.abstractmethod
    async def execute(self, context: ExecutionContext, node):
        """Executes the task and returns the result."""
        pass

    async def get_and_render_attributes(self, context: ExecutionContext, node):
        attributes = await self._get_attributes(node)
        cred_obj = await sync_to_async(node.workflow.get_primary_credential)() if hasattr(node, 'workflow') else None
        effective_service = await sync_to_async(lambda: node.task.effective_service)() if hasattr(node, 'task') else None
        template_context = self._build_template_context(context, node, cred_obj=cred_obj, effective_service=effective_service)
        return self._render_task_attributes(attributes, template_context)

    @sync_to_async
    def _get_attributes(self, node):
        return {ta.attribute.name: ta.attribute.value for ta in node.task.task_attributes.select_related('attribute')}

    def _build_template_context(self, context: ExecutionContext, node, cred_obj=None, effective_service=None) -> dict:
        parent_first = context.parent_results[0] if context.parent_results else {}
        payload = parent_first.get('response_json', {}) if isinstance(parent_first, dict) else {}
        node_result = getattr(node, 'primary_result', None) or context.get_result(node.slug) or {}

        last_sync_ts = getattr(node.workflow, 'last_sync_timestamp', None)
        if last_sync_ts and hasattr(last_sync_ts, 'isoformat'):
            last_sync_ts = last_sync_ts.isoformat()

        workflow_name = getattr(node.workflow, 'workflow_type', None)

        return {
            'parent': parent_first,
            'parents': context.parent_results or [],
            'payload': payload,
            'item': context.fanout_item,
            'workflow': {
                'organization_id': context.org_id,
                'last_sync_timestamp': last_sync_ts,
                'workflow_type': workflow_name,
            },
            'tasks': context.results,
            'task_params': context.get_task_params(node.slug),
            'result': node_result,
            'credential': cred_obj,
            'service': effective_service or {},
        }

    def _render_task_attributes(self, attr_map: Dict[str, Any], context: dict) -> Dict[str, Any]:
        rendered = {}
        for key, value in attr_map.items():
            rendered[key] = self._render_value(value, context)
        return rendered

    def _render_value(self, value: Any, context: dict) -> Any:
        if isinstance(value, str):
            return self._render_string(value, context)
        elif isinstance(value, dict):
            return {k: self._render_value(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._render_value(item, context) for item in value]
        else:
            return value

    def _render_string(self, template_str: str, context: dict) -> str:
        if '{{' not in template_str and '{%' not in template_str:
            return template_str
        try:
            from .jinja_helpers import get_jinja_env
            env = get_jinja_env()
            template = env.from_string(template_str)
            return template.render(**context)
        except Exception as e:
            logger.warning(f"Template rendering failed for '{template_str}': {e}")
            return template_str


class HttpExecutor(BaseExecutor):
    """Executor for HTTP tasks."""

    def __init__(self):
        rdb = get_async_redis()
        self.credential_manager = CredentialManager(rdb)

    async def execute(self, context: ExecutionContext, node):
        credential_id = None
        try:
            credential_id = await self.credential_manager.acquire(context, node)
            context.credentials['active'] = credential_id

            session = get_async_http_session()
            attributes = await self.get_and_render_attributes(context, node)

            method = attributes.get("method", "GET").upper()
            url = attributes.get("url")
            headers = attributes.get("headers", {})
            params = attributes.get("params", {})
            json_body = attributes.get("json")
            body = attributes.get("body")

            if not url:
                raise ValueError("URL is required for HTTP task")

            request_kwargs = {
                'method': method,
                'url': url,
                'headers': headers,
                'params': params,
            }
            content_type = str(headers.get('Content-Type', '')).lower()

            if body:
                if 'xml' in content_type:
                    request_kwargs['data'] = body
                else:
                    try:
                        request_kwargs['json'] = json.loads(body)
                    except (ValueError, TypeError):
                        request_kwargs['data'] = body
            elif json_body is not None:
                request_kwargs['json'] = json_body

            async with session.request(**request_kwargs) as resp:
                parsed_json = None
                try:
                    parsed_json = await resp.json()
                except Exception:
                    pass

                result = {
                    "status_code": resp.status,
                    "headers": dict(resp.headers),
                    "body": await resp.text(),
                    "response_json": parsed_json,
                    "status": "success" if 200 <= resp.status < 300 else "error"
                }
                
                # After successful request, increment usage and track egress
                if 200 <= resp.status < 300 and credential_id:
                    service_id = node.task.effective_service.id if node.task.effective_service else None
                    if service_id:
                        await increment_api_usage(credential_id, service_id, get_async_redis())
                        await track_data_egress(
                            task_id=f"{context.run_id}:{node.workflow.id}",
                            workflow_id=node.workflow.id,
                            credential_id=credential_id,
                            organization_id=context.org_id,
                            service_id=service_id,
                            request_size_bytes=len(json.dumps(json_body).encode('utf-8')) if json_body else 0,
                            response_size_bytes=len((await resp.text()).encode('utf-8')) if await resp.text() else 0,
                            url=url, method=method, status_code=resp.status,
                            run_id=context.run_id, redis=get_async_redis()
                        )

                return result
        finally:
            if credential_id:
                await self.credential_manager.release(credential_id, context, node)


class PythonExecutor(BaseExecutor):
    """Executor for Python tasks."""

    async def execute(self, context: ExecutionContext, node):
        attributes = await self.get_and_render_attributes(context, node)
        
        callable_path = attributes.get("python_callable")
        if not callable_path:
            raise ValueError("python_callable attribute is required for python task type")

        callable_kwargs = attributes.get("python_kwargs", {})
        if isinstance(callable_kwargs, str):
            callable_kwargs = json.loads(callable_kwargs)

        import importlib
        module_path, func_name = callable_path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        fn = getattr(mod, func_name)

        ctx = {"task": node.task, "context": context, "kwargs": callable_kwargs}
        
        import asyncio
        if asyncio.iscoroutinefunction(fn):
            result = await fn(ctx)
        else:
            result = await sync_to_async(fn)(ctx)

        if not isinstance(result, dict):
            result = {"value": result}
        
        return result


class MutationExecutor(BaseExecutor):
    """Executor for state mutation tasks."""

    async def execute(self, context: ExecutionContext, node):
        attributes = await self.get_and_render_attributes(context, node)
        mutations = attributes.get("mutations", {})
        if isinstance(mutations, str):
            mutations = json.loads(mutations)
        return mutations


class OrmExecutor(BaseExecutor):
    """Executor for ORM tasks."""

    async def execute(self, context: ExecutionContext, node):
        attributes = await self.get_and_render_attributes(context, node)

        model_name = attributes.get("orm_model")
        action = attributes.get("orm_action", "update").lower()
        orm_fields = attributes.get("orm_fields", {})
        orm_lookup = attributes.get("orm_lookup", {})

        if not model_name:
            raise ValueError("orm_model is required for ORM task")

        from django.apps import apps

        Model = apps.get_model(model_name)

        @sync_to_async
        def _execute_orm_action():
            if action == "update":
                instance, _ = Model.objects.update_or_create(defaults=orm_fields, **orm_lookup)
                return instance
            elif action == "create":
                instance = Model.objects.create(**orm_fields)
                return instance
            elif action == "get":
                return Model.objects.get(**orm_lookup)
            elif action == "filter":
                return list(Model.objects.filter(**orm_lookup).values())
        
        result = await _execute_orm_action()

        # For now, we just return a success message.
        # In the future, we might want to return the serialized object.
        return {"status": "success"}


class HumanInputExecutor(BaseExecutor):
    """Executor for human input required tasks."""

    async def execute(self, context: ExecutionContext, node):
        attributes = await self.get_and_render_attributes(context, node)
        return {
            "status": "awaiting_input",
            "prompt": attributes.get("prompt", ""),
            "input_key": attributes.get("input_key", "human_input"),
        }


class CheckpointExecutor(BaseExecutor):
    """Executor for checkpoint tasks."""

    async def execute(self, context: ExecutionContext, node):
        attributes = await self.get_and_render_attributes(context, node)
        return {
            "status": "checkpoint",
            "node_slug": node.slug,
        }


def get_executor(task_type: str) -> BaseExecutor:
    """
    Factory function to get the appropriate executor for a task type.
    Consults built-in executors first, then falls back to settings.TASK_EXECUTORS registry.
    """
    if task_type == "http":
        return HttpExecutor()
    elif task_type == "python":
        return PythonExecutor()
    elif task_type == "mutation":
        return MutationExecutor()
    elif task_type == "orm":
        return OrmExecutor()
    elif task_type == "human_input":
        return HumanInputExecutor()
    elif task_type == "checkpoint":
        return CheckpointExecutor()

    from django.conf import settings
    import importlib

    registry = getattr(settings, 'TASK_EXECUTORS', {}) or {}
    path = registry.get(task_type)
    if not path:
        raise ValueError(f"Unknown task type: {task_type}")

    try:
        mod_path, attr_name = path.rsplit('.', 1)
        mod = importlib.import_module(mod_path)
        cls = getattr(mod, attr_name)
        return cls()
    except (ValueError, ImportError, AttributeError) as e:
        raise ValueError(f"Could not load executor for task type '{task_type}' from '{path}': {e}")
