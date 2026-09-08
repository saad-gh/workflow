"""
Defines the ExecutionContext class, which holds the state of a workflow run.
"""
from typing import Any, Dict, List, Optional


class ExecutionContext:
    """
    Holds the state for a specific workflow run. This object is passed between
    executors and the traverser, accumulating results and state.
    """

    def __init__(self, run_id: str, org_id: int):
        self.run_id = run_id
        self.org_id = org_id
        self.results: Dict[str, Any] = {}
        self.task_params: Dict[str, Dict[str, Any]] = {}
        self.parent_results: Optional[List[Dict]] = None
        self.iteration_count = 0
        self.credentials: Dict[str, Any] = {}
        self.fanout_item: Optional[Any] = None

    def copy(self):
        new_context = ExecutionContext(self.run_id, self.org_id)
        new_context.results = self.results.copy()
        new_context.task_params = {k: v.copy() for k, v in self.task_params.items()}
        new_context.parent_results = self.parent_results.copy() if self.parent_results else None
        new_context.iteration_count = self.iteration_count
        new_context.credentials = self.credentials.copy()
        new_context.fanout_item = self.fanout_item
        return new_context

    def set_result(self, task_slug: str, result: Any):
        self.results[task_slug] = result

    def get_result(self, task_slug: str) -> Any:
        return self.results.get(task_slug)

    def get_task_params(self, task_slug: str) -> dict:
        return self.task_params.get(task_slug, {})

    def update_task_params(self, task_slug: str, params: dict):
        if isinstance(params, dict):
            if 'params' in params and isinstance(params['params'], dict):
                params = params['params']
            self.task_params.setdefault(task_slug, {}).update(params)
