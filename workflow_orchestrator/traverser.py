"""
Implements the WorkflowTraverser, which uses asyncio to traverse the DAG.
"""
import asyncio
import json
import logging
import time
import uuid
from asgiref.sync import sync_to_async

from .graph import WorkflowGraph, TaskNode
from .execution_context import ExecutionContext
from .telemetry import TelemetryLogger
from .executors import get_executor
from .quota import QuotaExceededError, NoAvailableCredentialsError
from .strategies import deduplicate_response_items
from .executors import get_async_redis


class WorkflowTraverser:
    """Traverses the workflow graph and executes tasks."""

    def __init__(self, graph: WorkflowGraph, context: ExecutionContext, logger: TelemetryLogger, max_execution_time: int = 3000):
        self.graph = graph
        self.context = context
        self.logger = logger
        self.start_time = time.time()
        self.max_execution_time = max_execution_time # In seconds

    async def traverse(self, node: TaskNode):
        """Main traversal logic for a single node."""
        if time.time() - self.start_time > self.max_execution_time:
            self.logger.log("NODE_PAUSED", node.slug, reason="max_execution_time_exceeded")
            await self._create_checkpoint(node, {"reason": "max_execution_time_exceeded"})
            # In a real implementation, we would enqueue a resume task.
            return

        self.logger.log("NODE_STARTED", node.slug)

        if not await self._execute_gatekeepers(node):
            self.logger.log("NODE_SKIPPED", node.slug, reason="Gatekeeper failed")
            return

        result = await self._execute_task(node)
        if result is None: # Task was paused or failed
            return
        self.context.set_result(node.slug, result)

        # Fan out the current page/result first, so that paginated tasks fan out
        # each page's items (rather than only the final page after the loop ends).
        await self._fanout_to_children(node, result)

        if await self._handle_continuations(node, result):
            return # The continuation will have already re-triggered the traversal

        self.logger.log("NODE_COMPLETED", node.slug)

    async def _execute_gatekeepers(self, node: TaskNode) -> bool:
        """
        Sequentially evaluates all WorkflowContinuation gatekeeper conditions for node.workflow.
        Returns True if ALL gatekeepers pass, False if ANY gatekeeper fails (signaling skip).
        """
        from asgiref.sync import sync_to_async
        from .jinja_helpers import get_jinja_env

        pre_conts = await sync_to_async(list)(
            node.workflow.pre_continuations.all().order_by('priority')
        )
        if not pre_conts:
            return True

        parent_first = self.context.parent_results[0] if self.context.parent_results else {}
        payload = parent_first.get('response_json', {}) if isinstance(parent_first, dict) else {}

        template_context = {
            'parent': parent_first,
            'parents': self.context.parent_results or [],
            'payload': payload,
            'item': self.context.fanout_item,
            'tasks': self.context.results,
            'workflow': {'organization_id': self.context.org_id},
        }

        env = get_jinja_env()

        for cont in pre_conts:
            cond_str = cont.condition
            if not cond_str or cond_str.strip().lower() == "true":
                continue
            try:
                template = env.from_string(cond_str)
                rendered = template.render(**template_context).strip().lower()
                if rendered not in ("true", "1", "yes"):
                    logger.warning(f"Gatekeeper failed for {node.slug}: condition='{cond_str}', rendered='{rendered}'")
                    return False
            except Exception as e:
                self.logger.log("GATEKEEPER_ERROR", node.slug, error=str(e))
                return False

        return True

    async def _execute_task(self, node: TaskNode):
        """Executes a single task."""
        try:
            executor = get_executor(node.task.task_type)
            return await executor.execute(self.context, node)
        except QuotaExceededError as e:
            self.logger.log("NODE_PAUSED", node.slug, reason="quota_exceeded")
            await self._create_checkpoint(node, e.quota_info)
            return None # Stop processing this branch
        except NoAvailableCredentialsError as e:
            self.logger.log("NODE_PAUSED", node.slug, reason="no_available_credentials")
            # In a real implementation, we would re-queue this task with a delay.
            return None
        except Exception as e:
            raise e

    async def _handle_continuations(self, node: TaskNode, result) -> bool:
        """Handles task continuations (looping). Returns True if a loop occurred."""
        for edge in sorted(node.self_edges, key=lambda e: e.priority):
            if edge.should_continue(self.context, result):
                self.context.iteration_count += 1
                self.logger.log("NODE_CONTINUATION", node.slug, iteration=self.context.iteration_count)

                cont_task = edge.continuation.continuation_task
                cont_node = TaskNode(node.workflow)
                cont_node.task = cont_task
                cont_node.slug = cont_task.slug
                cont_node.primary_result = result

                executor = get_executor(cont_task.task_type)
                cont_result = await executor.execute(self.context, cont_node)

                if cont_task.task_type == 'mutation' and isinstance(cont_result, dict):
                    self.context.update_task_params(node.slug, cont_result)

                await self.traverse(node) # Re-traverse the node
                return True
        return False

    async def _fanout_to_children(self, node: TaskNode, result):
        """Handles fanning out to child tasks."""
        if not result or result.get('status') != 'success':
            return

        items = await self._resolve_fanout_items(node, result)
        if items and node.task.deduplication_strategy != 'none':
            items = await deduplicate_response_items(items, node.task, self.context.org_id, get_async_redis())

        child_nodes = [edge.destination for edge in node.outgoing_edges]

        for child in child_nodes:
            if items is not None:
                for item in items:
                    new_context = self.context.copy() # Create a new context for each branch
                    new_context.fanout_item = item
                    new_context.parent_results = [result]
                    new_traverser = WorkflowTraverser(self.graph, new_context, self.logger)
                    await new_traverser.traverse(child)
            else:
                self.context.parent_results = [result]
                await self.traverse(child)
    
    @sync_to_async
    def _get_task_attribute(self, node: TaskNode, attr_name: str):
        ta = node.task.task_attributes.filter(attribute__name=attr_name).first()
        return ta.attribute.value if ta else None

    async def _resolve_fanout_items(self, node: TaskNode, result):
        fanout_path = await self._get_task_attribute(node, "fanout_path")
        if fanout_path and isinstance(result, dict):
            response_json = result.get('response_json', result)
            node = response_json
            for key in fanout_path.split('.'):
                if not isinstance(node, dict):
                    node = None
                    break
                node = node.get(key)
            if isinstance(node, list):
                return node

        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            if 'items' in result:
                return result['items']
            for val in result.values():
                if isinstance(val, list):
                    return val
        return None

    @sync_to_async
    def _create_checkpoint(self, node: TaskNode, quota_info: dict):
        """Creates a checkpoint for the current execution context."""
        from .models import Checkpoint
        import pickle
        
        # Don't save the graph in the context
        self.context.graph = None

        exceeded_rule = quota_info.get('exceeded_rule', {}) if isinstance(quota_info, dict) else {}
        rule_type = exceeded_rule.get('type') or quota_info.get('reason') or quota_info.get('rule_type') or 'unknown'

        Checkpoint.objects.create(
            workflow=node.workflow,
            node_slug=node.slug,
            context=pickle.dumps(self.context),
            reason=f"quota_exceeded: {rule_type}",
        )


async def start_workflow_run(
    organization_id: int,
    schedule_id: int = None,
    workflow_id: int = None,
    webhook_payload: dict = None
):
    """Starts and executes a workflow run."""
    from .models import Organization, Workflow
    
    run_id = str(uuid.uuid4())
    context = ExecutionContext(run_id, organization_id)

    org = await Organization.objects.aget(id=organization_id)
    graph = WorkflowGraph(org)
    await graph.build(schedule_id, workflow_id=workflow_id)
    
    if webhook_payload:
        context.parent_results = [{"response_json": webhook_payload}]

    logger = TelemetryLogger(run_id, organization_id)
    traverser = WorkflowTraverser(graph, context, logger)

    root_nodes = graph.get_root_nodes()
    await asyncio.gather(*(traverser.traverse(node) for node in root_nodes))

    # After a successful run, advance last_sync_timestamp for incremental (sync) workflows.
    await _finalize_sync_timestamps(root_nodes)

    return run_id


async def _finalize_sync_timestamps(root_nodes):
    """Advance last_sync_timestamp for completed sync workflows (no active checkpoints)."""
    from .models import Workflow, Checkpoint
    from django.utils import timezone

    def _touch():
        updated = 0
        for node in root_nodes:
            wf = node.workflow
            if wf.workflow_type != Workflow.TYPE_SYNC:
                continue
            if Checkpoint.objects.filter(workflow=wf).exists():
                continue
            wf.last_sync_timestamp = timezone.now()
            wf.save(update_fields=['last_sync_timestamp'])
            updated += 1
        return updated

    updated = await sync_to_async(_touch)()
    if updated:
        logging.getLogger(__name__).info(
            "Advanced last_sync_timestamp for %d sync workflow(s)", updated
        )

async def resume_run(checkpoint_id: int):
    """Resumes a workflow run from a checkpoint."""
    from .models import Checkpoint
    import pickle

    checkpoint = await Checkpoint.objects.aget(id=checkpoint_id)
    context = pickle.loads(checkpoint.context)

    graph = context.graph
    node = graph.nodes[checkpoint.node_slug]

    logger = TelemetryLogger(context.run_id, context.org_id)
    traverser = WorkflowTraverser(graph, context, logger)
    await traverser.traverse(node)
    
    # After resuming, we can optionally delete the checkpoint or mark it inactive.
    # For now, let's delete it since it's been resumed.
    await checkpoint.adelete()

    return context.run_id
