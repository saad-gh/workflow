"""
Mock server infrastructure for testing the workflow orchestration engine.

Two complementary patching strategies:

1. Full patch — AiohttpMocker
   Patches aiohttp.ClientSession.request() entirely.  No network I/O.
   Best for fast unit/integration tests of continuation, retry, and DAG logic.

   Usage::

       from workflow_orchestrator.mock_server import AiohttpMocker

       with AiohttpMocker() as mocker:
           asyncio.run(execute_tasks_in_parallel([task_config], concurrency=1))

       assert mocker.call_count('shopify_list_orders') == 3   # 3 pages
       assert mocker.last_status('shopify_list_orders') == 200

2. Partial patch — mock_response_view + URL rewrite
   The real Django/aiohttp stack runs end-to-end; only the external API URL
   is swapped to point at the built-in mock view.  Best for testing the full
   executor→orchestrator loop with real Redis.

   Task setup::

       task.set_attribute('url', 'http://testserver/task/mock/shopify_list_orders/')

   Register in urls.py (already done)::

       path('mock/<slug:task_slug>/', mock_response_view, name='mock_response'),

Shared between both strategies: MockTaskResponse (models.py) holds the
canned responses; call counts are tracked in memory (AiohttpMocker) or in
the module-level _VIEW_CALL_COUNTS dict (Django view).
"""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, Optional
from unittest.mock import AsyncMock, patch

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-view call count registry (used by mock_response_view)
# Reset between tests with reset_view_call_counts()
# ---------------------------------------------------------------------------
_VIEW_CALL_COUNTS: dict[str, int] = defaultdict(int)


def reset_view_call_counts() -> None:
    """Call this in setUp() / tearDown() to isolate test state."""
    _VIEW_CALL_COUNTS.clear()


# ---------------------------------------------------------------------------
# Fake aiohttp response object
# ---------------------------------------------------------------------------

class MockAiohttpResponse:
    """
    Minimal async context manager that mimics aiohttp.ClientResponse.

    Only the attributes read by _execute_single_task are implemented:
        resp.status          → int
        resp.headers         → dict
        await resp.text()    → str
        await resp.json()    → Any
    """

    def __init__(self, status_code: int, body: Any, headers: dict, delay_ms: int = 0):
        self.status = status_code
        self._body = body
        self.headers = headers
        self._delay_ms = delay_ms

    async def text(self) -> str:
        if self._delay_ms:
            await asyncio.sleep(self._delay_ms / 1000)
        if self._body is None:
            return ''
        return json.dumps(self._body)

    async def json(self, content_type=None) -> Any:
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


# ---------------------------------------------------------------------------
# Scenario selection logic (shared between mocker and view)
# ---------------------------------------------------------------------------

def _find_best_scenario(task_slug: str, method: str, params: dict):
    """
    Return the best-matching active MockTaskResponse for the given request,
    or None if no scenario matches.

    Selection algorithm:
      1. Filter to active scenarios for this task slug.
      2. Filter to those whose match_method and match_params satisfy the request.
      3. Sort by (priority DESC, specificity DESC) — first result wins.
    """
    from workflow_orchestrator.models import MockTaskResponse  # avoid circular import

    candidates = (
        MockTaskResponse.objects
        .filter(task__slug=task_slug, is_active=True)
        .select_related('task')
    )

    matching = [s for s in candidates if s.matches_request(method, params)]
    if not matching:
        return None

    matching.sort(key=lambda s: (s.priority, s.specificity), reverse=True)
    return matching[0]


# ---------------------------------------------------------------------------
# 1. Full patch: AiohttpMocker
# ---------------------------------------------------------------------------

class AiohttpMocker:
    """
    Context manager that replaces aiohttp.ClientSession.request with a
    callable backed by MockTaskResponse records in the database.

    Task slug extraction from URL:
        The mocker expects task URLs to follow the pattern
        ``http://mock/<task_slug>/...`` (set this in your test fixtures).
        The slug is extracted from the path's first segment after the host.

    Call tracking:
        self._calls[task_slug]  →  int   (total calls for that slug)
        self._statuses[task_slug]  →  list[int]   (status codes in order)

    Example::

        with AiohttpMocker() as mocker:
            asyncio.run(execute_tasks_in_parallel([...]))

        assert mocker.call_count('shopify_list_orders') == 2
        assert mocker.statuses('shopify_list_orders') == [200, 200]
    """

    MOCK_HOST = 'mock'  # URL host used in test fixture URLs

    def __init__(self):
        self._calls: dict[str, int] = defaultdict(int)
        self._statuses: dict[str, list] = defaultdict(list)
        self._urls: dict[str, list] = defaultdict(list)
        self._patcher = None
        # Scenarios pre-loaded from DB on __enter__ (sync context) so that
        # request() never touches the ORM from inside an async view.
        self._scenarios: dict[str, list] = {}  # slug → [MockTaskResponse, ...]
        self._url_to_slug: dict[str, str] = {}  # url → slug

    # -- public API ---------------------------------------------------------

    def call_count(self, task_slug: str) -> int:
        return self._calls[task_slug]

    def statuses(self, task_slug: str) -> list:
        return self._statuses[task_slug]

    def last_status(self, task_slug: str) -> Optional[int]:
        s = self._statuses[task_slug]
        return s[-1] if s else None

    def last_url(self, task_slug: str) -> Optional[str]:
        u = self._urls.get(task_slug, [])
        return u[-1] if u else None

    # -- context manager ----------------------------------------------------

    def __enter__(self):
        # Load all active scenarios from DB now, while we're in a sync context.
        # This avoids any ORM call from inside the async view's request handler.
        self._load_scenarios()
        self._patcher = patch(
            'workflow_orchestrator.executors.aiohttp.ClientSession',
            new=self._make_client_session_mock(),
        )
        self._patcher.start()
        return self

    def __exit__(self, *_):
        if self._patcher:
            self._patcher.stop()

    # -- internal -----------------------------------------------------------

    def _load_scenarios(self):
        """
        Pull all active MockTaskResponse rows from the DB and group by slug.
        Called synchronously in __enter__ before the async context starts.
        """
        from workflow_orchestrator.models import MockTaskResponse
        self._scenarios.clear()
        self._url_to_slug.clear()

        for scenario in MockTaskResponse.objects.filter(is_active=True).select_related('task'):
            slug = scenario.task.slug
            self._scenarios.setdefault(slug, []).append(scenario)

            # Map task's (method, URL) attribute to task slug for URL matching
            url_ta = scenario.task.task_attributes.filter(attribute__name='url').first()
            method_ta = scenario.task.task_attributes.filter(attribute__name='method').first()
            method = method_ta.attribute.value.upper() if method_ta and isinstance(method_ta.attribute.value, str) else 'GET'

            if url_ta and isinstance(url_ta.attribute.value, str):
                target_url = url_ta.attribute.value.split('?')[0].rstrip('/')
                self._url_to_slug[(method, target_url)] = slug
                self._url_to_slug[target_url] = slug

        # Sort each slug's scenarios by (priority DESC, specificity DESC)
        for slug in self._scenarios:
            self._scenarios[slug].sort(
                key=lambda s: (s.priority, s.specificity), reverse=True
            )

    def _find_scenario(self, task_slug: str, method: str, params: dict):
        """In-memory scenario lookup — no ORM, safe to call from async context."""
        candidates = self._scenarios.get(task_slug, [])
        matching = [s for s in candidates if s.matches_request(method, params)]
        return matching[0] if matching else None

    def _slug_from_url(self, url: str, method: str = 'GET') -> Optional[str]:
        """
        Extract task slug from a URL of the form http://mock/<slug>/...
        Or match against known active scenario task slugs if URL contains it.
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if parsed.hostname == self.MOCK_HOST:
                parts = [p for p in parsed.path.split('/') if p]
                if parts:
                    return parts[0]

            clean_url = url.split('?')[0].rstrip('/')
            method_upper = method.upper() if method else 'GET'

            if (method_upper, clean_url) in self._url_to_slug:
                return self._url_to_slug[(method_upper, clean_url)]

            if clean_url in self._url_to_slug and isinstance(self._url_to_slug[clean_url], str):
                return self._url_to_slug[clean_url]

            for key, slug in self._url_to_slug.items():
                if isinstance(key, tuple):
                    m, u = key
                    if m == method_upper and (clean_url.startswith(u) or u.startswith(clean_url)):
                        return slug

            # Fallback: match URL against known scenario task slugs
            for slug in self._scenarios:
                if slug in url or slug.replace('_', '-') in url or slug.replace('-', '_') in url:
                    return slug

            return None
        except Exception:
            return None

    def _make_client_session_mock(self):
        """
        Build a replacement for aiohttp.ClientSession as used in task.views.

        Patching the whole class (rather than just the request method) avoids
        descriptor/coroutine-wrapper issues in aiohttp's _RequestContextManager
        and follows the Python convention of patching where something is *used*.

        The mock class supports:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, ...) as resp:
        """
        mocker = self  # captured reference

        class MockClientSession:
            """Drop-in for aiohttp.ClientSession inside task.views."""
            def __init__(self, *args, **kwargs):
                self.closed = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            def post(self_session, url, **kwargs):
                return self_session.request("POST", url, **kwargs)

            def get(self_session, url, **kwargs):
                return self_session.request("GET", url, **kwargs)

            def put(self_session, url, **kwargs):
                return self_session.request("PUT", url, **kwargs)

            def delete(self_session, url, **kwargs):
                return self_session.request("DELETE", url, **kwargs)

            def patch(self_session, url, **kwargs):
                return self_session.request("PATCH", url, **kwargs)

            def request(self_session, method, url, *, params=None, **kwargs):
                params = params or {}
                task_slug = mocker._slug_from_url(url, method=method)

                if task_slug is None:
                    logger.warning(
                        f"AiohttpMocker: URL '{url}' does not match mock pattern "
                        f"(expected http://{AiohttpMocker.MOCK_HOST}/<slug>/...). "
                        "Returning 404."
                    )
                    return MockAiohttpResponse(404, None, {})

                scenario = mocker._find_scenario(task_slug, method, params)

                if scenario is None:
                    logger.warning(
                        f"AiohttpMocker: No active MockTaskResponse for slug='{task_slug}' "
                        f"method={method} params={params}. Returning 404. "
                        f"Available slugs: {list(mocker._scenarios.keys())}"
                    )
                    return MockAiohttpResponse(404, None, {})

                call_idx = mocker._calls[task_slug]
                response_spec = scenario.get_response_for_call(call_idx)

                mocker._calls[task_slug] += 1
                mocker._statuses[task_slug].append(response_spec.get('status_code', 200))
                mocker._urls[task_slug].append(url)

                logger.debug(
                    f"AiohttpMocker: {method} {url} → scenario='{scenario.scenario}' "
                    f"call={call_idx} status={response_spec.get('status_code')}"
                )

                return MockAiohttpResponse(
                    status_code=response_spec.get('status_code', 200),
                    body=response_spec.get('body'),
                    headers=response_spec.get('headers', {}),
                    delay_ms=response_spec.get('delay_ms', 0),
                )

        return MockClientSession


# ---------------------------------------------------------------------------
# 2. Partial patch: Django mock response view
# ---------------------------------------------------------------------------

@csrf_exempt
@require_http_methods(["GET", "POST", "PUT", "PATCH", "DELETE"])
@csrf_exempt
@require_http_methods(["GET", "POST", "PUT", "PATCH", "DELETE"])
def mock_response_view(request, task_slug: str):
    """
    Django view that serves MockTaskResponse records for a given task slug.

    Register in urls.py::

        path('mock/<slug:task_slug>/', mock_response_view, name='mock_response'),

    Task URL for partial patching::

        task.set_attribute('url', 'http://testserver/task/mock/shopify_list_orders/')

    Query params are forwarded from the executor to this view and used for
    scenario matching (e.g. ``?cursor=abc123`` selects the page-2 scenario).

    Call counts are tracked in the module-level _VIEW_CALL_COUNTS dict.
    Reset between tests by calling reset_view_call_counts().
    """
    method = request.method
    params = dict(request.GET)
    # Flatten single-value lists from QueryDict
    params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v
              for k, v in params.items()}

    scenario = _find_best_scenario(task_slug, method, params)

    if scenario is None:
        return JsonResponse(
            {'error': f"No active MockTaskResponse for task_slug='{task_slug}'"},
            status=404,
        )

    call_key = f"{task_slug}:{scenario.scenario}"
    call_idx = _VIEW_CALL_COUNTS[call_key]
    response_spec = scenario.get_response_for_call(call_idx)
    _VIEW_CALL_COUNTS[call_key] += 1

    delay_ms = response_spec.get('delay_ms', 0)
    if delay_ms:
        import time
        time.sleep(delay_ms / 1000)

    body = response_spec.get('body')
    status_code = response_spec.get('status_code', 200)
    extra_headers = response_spec.get('headers', {})

    response = JsonResponse(body if body is not None else {}, status=status_code, safe=False)
    for header_name, header_value in extra_headers.items():
        response[header_name] = header_value

    return response


# ---------------------------------------------------------------------------
# Builder helper — fluent API for constructing scenarios in tests
# ---------------------------------------------------------------------------

class ScenarioBuilder:
    """
    Fluent helper for creating MockTaskResponse records in test setUp.

    Example::

        (ScenarioBuilder(task, 'paginated_cursor')
            .description('Two-page Shopify orders response')
            .on_call(0).returns(200, {'orders': [...], 'next_cursor': 'tok1'})
            .on_params({'page_info': 'tok1'}).returns(200, {'orders': [...], 'next_cursor': None})
            .save())

    More commonly, use the simpler helpers::

        ScenarioBuilder.success(task, body={'orders': [...]})
        ScenarioBuilder.rate_limited(task, retry_after=30)
        ScenarioBuilder.paginated_cursor(task, pages=[page1_body, page2_body])
        ScenarioBuilder.unauthorized(task)
    """

    def __init__(self, task, scenario: str):
        self._task = task
        self._scenario = scenario
        self._description = ''
        self._match_method = ''
        self._match_params: dict = {}
        self._priority = 0
        self._responses: list = []

    def description(self, text: str) -> 'ScenarioBuilder':
        self._description = text
        return self

    def method(self, m: str) -> 'ScenarioBuilder':
        self._match_method = m.upper()
        return self

    def match(self, **params) -> 'ScenarioBuilder':
        self._match_params = params
        return self

    def priority(self, p: int) -> 'ScenarioBuilder':
        self._priority = p
        return self

    def respond(self, status_code: int, body=None, headers=None, delay_ms=0) -> 'ScenarioBuilder':
        self._responses.append({
            'status_code': status_code,
            'body': body or {},
            'headers': headers or {},
            'delay_ms': delay_ms,
        })
        return self

    def save(self):
        from workflow_orchestrator.models import MockTaskResponse
        obj, _ = MockTaskResponse.objects.update_or_create(
            task=self._task,
            scenario=self._scenario,
            defaults=dict(
                description=self._description,
                match_method=self._match_method,
                match_params=self._match_params,
                priority=self._priority,
                responses=self._responses,
                is_active=True,
            ),
        )
        return obj

    # -- common canned scenarios -------------------------------------------

    @classmethod
    def success(cls, task, body: dict, scenario='success', headers=None):
        """Single 200 OK response."""
        return (
            cls(task, scenario)
            .respond(200, body, headers or {})
            .save()
        )

    @classmethod
    def error(cls, task, status_code: int, body=None, scenario=None):
        """Single error response."""
        scenario = scenario or f'error_{status_code}'
        return (
            cls(task, scenario)
            .respond(status_code, body or {'error': 'mock error'})
            .save()
        )

    @classmethod
    def rate_limited_then_success(cls, task, success_body: dict, retry_after: int = 1):
        """
        First call → 429 with Retry-After header.
        Subsequent calls → 200 OK.
        """
        return (
            cls(task, 'rate_limited_then_success')
            .description(f'429 on first call, 200 thereafter (Retry-After: {retry_after}s)')
            .respond(429, {'error': 'rate limited'}, {'Retry-After': str(retry_after)})
            .respond(200, success_body)
            .save()
        )

    @classmethod
    def unauthorized_then_success(cls, task, success_body: dict):
        """
        First call → 401 Unauthorized (triggers credential refresh).
        Subsequent calls → 200 OK.
        """
        return (
            cls(task, 'unauthorized_then_success')
            .description('401 on first call to trigger credential refresh, 200 thereafter')
            .respond(401, {'error': 'token expired'})
            .respond(200, success_body)
            .save()
        )

    @classmethod
    def paginated_cursor(cls, task, pages: list, cursor_field='next_cursor',
                         cursor_param='cursor'):
        """
        Create one scenario per page.  Each page (except the last) embeds a
        cursor in its body; the next scenario matches that cursor in params.

        pages: list of body dicts.  The builder automatically injects
        ``cursor_field`` into all but the last page.
        """
        from workflow_orchestrator.models import MockTaskResponse

        cursors = [f'cursor_page_{i + 2}' for i in range(len(pages) - 1)]

        for i, body in enumerate(pages):
            is_last = i == len(pages) - 1
            full_body = dict(body)
            if not is_last:
                full_body[cursor_field] = cursors[i]
            else:
                full_body[cursor_field] = None

            match_params = {cursor_param: cursors[i - 1]} if i > 0 else {}

            (cls(task, f'page_{i + 1}')
             .description(f'Page {i + 1} of {len(pages)}')
             .match(**match_params)
             .priority(i)   # page_1 has lowest priority (matches anything)
             .respond(200, full_body)
             .save())

    @classmethod
    def paginated_offset(cls, task, pages: list, page_size: int = 100):
        """
        Create one scenario per page using offset/limit pagination.
        Dynamically calculates expected request offsets based on actual previous page sizes
        to handle variable page-size scenarios safely without off-by-one errors.
        Stops creating subsequent page matchers if a partial page (less than page_size) is encountered.
        """
        cumulative_offset = 0
        for i, body in enumerate(pages):
            match_params = {'offset': str(cumulative_offset)} if cumulative_offset > 0 else {}

            (cls(task, f'offset_page_{i + 1}')
             .description(f'Offset page {i + 1} (offset={cumulative_offset})')
             .match(**match_params)
             .priority(i)
             .respond(200, body)
             .save())

            # Calculate next page's expected offset based on actual items in the current page
            items = body.get('items') or body.get('data') or []
            item_count = len(items)
            
            # If the current page is partial (less than the requested page_size), 
            # pagination must halt naturally. Do not construct matchers for subsequent pages.
            if item_count < page_size:
                break
                
            cumulative_offset += item_count
