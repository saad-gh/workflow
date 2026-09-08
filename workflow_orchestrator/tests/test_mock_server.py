"""
Tests for the mock server infrastructure itself.

Covers:
  - MockTaskResponse model: scenario matching, specificity, sequence repetition
  - _find_best_scenario: priority order, param specificity, method filter
  - AiohttpMocker: call count tracking, URL routing, 404 on unknown slug
  - mock_response_view (Django view): correct response served, call count increments
  - ScenarioBuilder helpers: success, error, rate_limited_then_success, etc.
  - reset_view_call_counts: state isolation between tests
"""

import asyncio
import json

from django.test import TestCase, Client

from workflow_orchestrator.models import MockTaskResponse
from workflow_orchestrator.mock_server import (
    AiohttpMocker,
    ScenarioBuilder,
    _find_best_scenario,
    reset_view_call_counts,
    _VIEW_CALL_COUNTS,
)
from workflow_orchestrator.tests.base import WorkflowTestCase, make_task


# ---------------------------------------------------------------------------
# MockTaskResponse model unit tests
# ---------------------------------------------------------------------------

class MockTaskResponseModelTest(TestCase):

    def setUp(self):
        self.task = make_task('model_test_task')

    def _create(self, scenario, match_method='', match_params=None, priority=0, responses=None):
        return MockTaskResponse.objects.create(
            task=self.task,
            scenario=scenario,
            match_method=match_method,
            match_params=match_params or {},
            priority=priority,
            responses=responses or [{'status_code': 200, 'body': {}, 'headers': {}}],
        )

    # -- matches_request ---------------------------------------------------

    def test_blank_method_matches_any(self):
        s = self._create('s1', match_method='')
        self.assertTrue(s.matches_request('GET', {}))
        self.assertTrue(s.matches_request('POST', {}))

    def test_method_filter_case_insensitive(self):
        s = self._create('s2', match_method='GET')
        self.assertTrue(s.matches_request('get', {}))
        self.assertTrue(s.matches_request('GET', {}))

    def test_method_mismatch_fails(self):
        s = self._create('s3', match_method='POST')
        self.assertFalse(s.matches_request('GET', {}))

    def test_empty_params_matches_anything(self):
        s = self._create('s4', match_params={})
        self.assertTrue(s.matches_request('GET', {'cursor': 'abc', 'limit': '100'}))

    def test_specific_param_must_match(self):
        s = self._create('s5', match_params={'cursor': 'abc'})
        self.assertTrue(s.matches_request('GET', {'cursor': 'abc', 'limit': '100'}))
        self.assertFalse(s.matches_request('GET', {'cursor': 'xyz'}))
        self.assertFalse(s.matches_request('GET', {}))

    def test_all_match_params_must_be_present(self):
        s = self._create('s6', match_params={'page': '2', 'sort': 'asc'})
        self.assertFalse(s.matches_request('GET', {'page': '2'}))  # missing sort
        self.assertTrue(s.matches_request('GET', {'page': '2', 'sort': 'asc'}))

    # -- specificity -------------------------------------------------------

    def test_specificity_is_param_count(self):
        s0 = self._create('sp0', match_params={})
        s2 = self._create('sp2', match_params={'a': '1', 'b': '2'})
        self.assertEqual(s0.specificity, 0)
        self.assertEqual(s2.specificity, 2)

    # -- get_response_for_call ---------------------------------------------

    def test_returns_first_response_on_call_0(self):
        s = self._create('resp_seq', responses=[
            {'status_code': 429, 'body': {}, 'headers': {}},
            {'status_code': 200, 'body': {'ok': True}, 'headers': {}},
        ])
        r = s.get_response_for_call(0)
        self.assertEqual(r['status_code'], 429)

    def test_returns_second_response_on_call_1(self):
        s = self._create('resp_seq2', responses=[
            {'status_code': 429, 'body': {}, 'headers': {}},
            {'status_code': 200, 'body': {'ok': True}, 'headers': {}},
        ])
        self.assertEqual(s.get_response_for_call(1)['status_code'], 200)

    def test_last_response_repeated_when_exhausted(self):
        s = self._create('resp_repeat', responses=[
            {'status_code': 429, 'body': {}, 'headers': {}},
            {'status_code': 200, 'body': {}, 'headers': {}},
        ])
        for i in range(2, 10):
            r = s.get_response_for_call(i)
            self.assertEqual(r['status_code'], 200, f"call {i} should repeat last (200)")

    def test_empty_responses_returns_default_200(self):
        s = self._create('empty_resp', responses=[])
        r = s.get_response_for_call(0)
        self.assertEqual(r['status_code'], 200)


# ---------------------------------------------------------------------------
# _find_best_scenario
# ---------------------------------------------------------------------------

class FindBestScenarioTest(TestCase):

    def setUp(self):
        self.task = make_task('routing_task')

    def _create(self, scenario, match_method='', match_params=None, priority=0):
        return MockTaskResponse.objects.create(
            task=self.task,
            scenario=scenario,
            match_method=match_method,
            match_params=match_params or {},
            priority=priority,
            responses=[{'status_code': 200, 'body': {'scenario': scenario}, 'headers': {}}],
        )

    def test_returns_none_when_no_scenarios(self):
        result = _find_best_scenario('nonexistent_task', 'GET', {})
        self.assertIsNone(result)

    def test_returns_generic_scenario(self):
        self._create('default')
        result = _find_best_scenario('routing_task', 'GET', {})
        self.assertIsNotNone(result)
        self.assertEqual(result.scenario, 'default')

    def test_specific_params_beat_generic(self):
        self._create('generic', match_params={})
        self._create('page2', match_params={'cursor': 'tok'})

        result = _find_best_scenario('routing_task', 'GET', {'cursor': 'tok'})
        self.assertEqual(result.scenario, 'page2')

    def test_generic_selected_when_no_params(self):
        self._create('generic', match_params={}, priority=0)
        self._create('page2', match_params={'cursor': 'tok'}, priority=5)

        result = _find_best_scenario('routing_task', 'GET', {})
        self.assertEqual(result.scenario, 'generic')

    def test_priority_field_overrides_specificity(self):
        """Explicit priority beats specificity — allows overriding matching order."""
        self._create('low_specific', match_params={'a': '1', 'b': '2'}, priority=0)
        self._create('high_priority', match_params={'a': '1'}, priority=100)

        result = _find_best_scenario('routing_task', 'GET', {'a': '1', 'b': '2'})
        self.assertEqual(result.scenario, 'high_priority')

    def test_inactive_scenario_excluded(self):
        self._create('active')
        MockTaskResponse.objects.create(
            task=self.task,
            scenario='inactive',
            is_active=False,
            responses=[{'status_code': 500, 'body': {}, 'headers': {}}],
        )
        result = _find_best_scenario('routing_task', 'GET', {})
        self.assertEqual(result.scenario, 'active')

    def test_method_filter_respected(self):
        self._create('get_only', match_method='GET')
        self._create('post_only', match_method='POST')
        get_result = _find_best_scenario('routing_task', 'GET', {})
        post_result = _find_best_scenario('routing_task', 'POST', {})
        self.assertEqual(get_result.scenario, 'get_only')
        self.assertEqual(post_result.scenario, 'post_only')


# ---------------------------------------------------------------------------
# AiohttpMocker
# ---------------------------------------------------------------------------

class AiohttpMockerTest(WorkflowTestCase):

    def setUp(self):
        super().setUp()
        self.task = make_task('mocker_task')
        ScenarioBuilder.success(self.task, {'orders': [1, 2, 3]})

    def test_call_count_starts_at_zero(self):
        self.assertEqual(self.mocker.call_count('mocker_task'), 0)

    def test_unknown_slug_returns_404(self):
        """AiohttpMocker returns 404 for URLs not matching a known task slug."""
        async def _call():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.request('GET', 'http://mock/unknown_slug/') as resp:
                    return resp.status

        with self.mocker:
            status = self.run_async(_call())
        self.assertEqual(status, 404)

    def test_known_slug_returns_200(self):
        async def _call():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.request('GET', 'http://mock/mocker_task/') as resp:
                    return resp.status

        with self.mocker:
            status = self.run_async(_call())
        self.assertEqual(status, 200)

    def test_call_count_increments(self):
        async def _call_three():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                for _ in range(3):
                    async with session.request('GET', 'http://mock/mocker_task/') as resp:
                        pass

        with self.mocker:
            self.run_async(_call_three())
        self.assertEqual(self.mocker.call_count('mocker_task'), 3)

    def test_statuses_tracked(self):
        ScenarioBuilder.rate_limited_then_success(self.task, {'ok': True})

        task2 = make_task('mocker_task2')
        ScenarioBuilder.rate_limited_then_success(task2, {'ok': True})

        async def _call():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.request('GET', 'http://mock/mocker_task2/') as r1:
                    s1 = r1.status
                async with session.request('GET', 'http://mock/mocker_task2/') as r2:
                    s2 = r2.status
            return s1, s2

        with self.mocker:
            s1, s2 = self.run_async(_call())
        self.assertEqual(s1, 429)
        self.assertEqual(s2, 200)

    def test_response_body_accessible(self):
        async def _call():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.request('GET', 'http://mock/mocker_task/') as resp:
                    return await resp.json()

        with self.mocker:
            body = self.run_async(_call())
        self.assertEqual(body, {'orders': [1, 2, 3]})


# ---------------------------------------------------------------------------
# mock_response_view (partial patch)
# ---------------------------------------------------------------------------

class MockResponseViewTest(TestCase):

    def setUp(self):
        self.client = Client()
        self.task = make_task('view_test_task')
        reset_view_call_counts()

    def tearDown(self):
        reset_view_call_counts()

    def test_success_response_served(self):
        ScenarioBuilder.success(self.task, {'data': 'hello'})
        response = self.client.get('/task/mock/view_test_task/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content), {'data': 'hello'})

    def test_404_when_no_scenarios(self):
        response = self.client.get('/task/mock/no_such_task/')
        self.assertEqual(response.status_code, 404)

    def test_custom_status_code_served(self):
        ScenarioBuilder.error(self.task, 422, {'message': 'unprocessable'})
        response = self.client.get('/task/mock/view_test_task/')
        self.assertEqual(response.status_code, 422)

    def test_rate_limit_response_has_retry_after_header(self):
        ScenarioBuilder.rate_limited_then_success(self.task, {'ok': True}, retry_after=60)
        response = self.client.get('/task/mock/view_test_task/')
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response['Retry-After'], '60')

    def test_second_call_serves_next_in_sequence(self):
        ScenarioBuilder.rate_limited_then_success(self.task, {'ok': True})
        r1 = self.client.get('/task/mock/view_test_task/')
        r2 = self.client.get('/task/mock/view_test_task/')
        self.assertEqual(r1.status_code, 429)
        self.assertEqual(r2.status_code, 200)

    def test_param_based_routing(self):
        """Cursor param selects page-2 scenario."""
        pages = [{'items': [1]}, {'items': [2]}]
        ScenarioBuilder.paginated_cursor(
            self.task, pages, cursor_field='next', cursor_param='after'
        )
        page1 = self.client.get('/task/mock/view_test_task/')
        cursor = json.loads(page1.content).get('next')

        page2 = self.client.get(f'/task/mock/view_test_task/?after={cursor}')
        body2 = json.loads(page2.content)
        self.assertEqual(body2['items'], [2])

    def test_reset_view_call_counts_clears_state(self):
        ScenarioBuilder.rate_limited_then_success(self.task, {'ok': True})
        self.client.get('/task/mock/view_test_task/')  # call 0 → 429
        reset_view_call_counts()
        response = self.client.get('/task/mock/view_test_task/')  # should be 429 again
        self.assertEqual(response.status_code, 429)
