"""
Defines the Object-Oriented data structures for representing the workflow as a graph.
"""
import asyncio
from collections import defaultdict
from typing import Dict, List, Optional

from asgiref.sync import sync_to_async

from .models import Workflow, Task, WorkflowContinuation, TaskContinuation


class TaskNode:
    """Represents a single task in the workflow graph."""

    def __init__(self, workflow: Workflow):
        self.workflow = workflow
        self.task: Task = workflow.task
        self.id = self.task.id
        self.slug = self.task.slug
        self.outgoing_edges: List['Edge'] = []
        self.self_edges: List['TaskContinuationEdge'] = []
        self.executor = None  # To be bound dynamically

    def add_outgoing_edge(self, edge: 'Edge'):
        self.outgoing_edges.append(edge)

    def add_self_edge(self, edge: 'TaskContinuationEdge'):
        self.self_edges.append(edge)


class Edge:
    """Represents a directed connection between two TaskNodes in the DAG."""

    def __init__(self, source: TaskNode, destination: TaskNode):
        self.source = source
        self.destination = destination


from jinja2 import Environment

class TaskContinuationEdge:
    """Represents a self-loop on a TaskNode for retries or pagination."""

    def __init__(self, node: TaskNode, continuation: TaskContinuation):
        self.node = node
        self.continuation = continuation
        self.priority = continuation.priority

    def should_continue(self, context, result) -> bool:
        template_context = {
            "result": result,
            "task_params": context.get_task_params(self.node.slug),
            "workflow": {
                "organization_id": context.org_id,
            },
            "service": {},
        }

        try:
            env = Environment()
            template = env.from_string(self.continuation.condition)
            rendered_cond = template.render(**template_context)
            return rendered_cond.strip().lower() in ("true", "1", "yes")
        except Exception:
            return False


class WorkflowGraph:
    """Builds and holds the DAG of TaskNodes and Edges."""

    def __init__(self, organization):
        self.organization = organization
        self.nodes: Dict[int, TaskNode] = {}
        self.root_nodes: List[TaskNode] = []

    async def build(self, schedule=None, workflow_id=None):
        wf_by_task_id, children, parents, scheduled_workflow_ids = await self._build_task_graph_for_org(schedule)

        # Create all nodes
        for task_id, wf in wf_by_task_id.items():
            self.nodes[task_id] = TaskNode(wf)

        # Connect nodes with directed edges based on mapped_by children topology
        for parent_task_id, child_task_ids in children.items():
            parent_node = self.nodes.get(parent_task_id)
            if not parent_node:
                continue

            for child_task_id in child_task_ids:
                child_node = self.nodes.get(child_task_id)
                if not child_node:
                    continue
                
                edge = Edge(parent_node, child_node)
                parent_node.add_outgoing_edge(edge)

        # Add self-edges (TaskContinuations)
        for node in self.nodes.values():
            for cont in await sync_to_async(list)(TaskContinuation.objects.filter(primary_task=node.task).select_related('continuation_task')):
                edge = TaskContinuationEdge(node, cont)
                node.add_self_edge(edge)
        
        # Identify root nodes
        root_task_ids = [tid for tid in self.nodes.keys() if not parents.get(tid)]
        if workflow_id:
            self.root_nodes = [self.nodes[tid] for tid in root_task_ids if self.nodes[tid].workflow.id == workflow_id]
        elif scheduled_workflow_ids:
            self.root_nodes = [self.nodes[tid] for tid in root_task_ids if self.nodes[tid].workflow.id in scheduled_workflow_ids]
        else:
            self.root_nodes = [self.nodes[tid] for tid in root_task_ids]

    async def _build_task_graph_for_org(self, schedule=None):
        wf_qs = Workflow.objects.filter(organization=self.organization)

        # If a schedule is provided, we still need to build the full graph to resolve downstream dependencies
        # but we will only start the traversal from the roots of the scheduled workflows.
        scheduled_workflow_ids = None
        if schedule is not None:
            scheduled_workflow_ids = await sync_to_async(list)(wf_qs.filter(schedule=schedule).values_list('id', flat=True))

        wf_qs = wf_qs.select_related('task__resource__service', 'task__service', 'organization', 'schedule').prefetch_related('mapped_by')

        all_workflows = await sync_to_async(list)(wf_qs)
        wf_by_task_id: Dict[int, Workflow] = {wf.task_id: wf for wf in all_workflows}

        children: Dict[int, List[int]] = defaultdict(list)
        parents: Dict[int, List[int]] = defaultdict(list)

        # Build DAG topology strictly from Workflow.mapped_by!
        for wf in all_workflows:
            child_task_id = wf.task_id
            for parent_wf in wf.mapped_by.all():
                if parent_wf.task_id in wf_by_task_id:
                    parent_task_id = parent_wf.task_id
                    children[parent_task_id].append(child_task_id)
                    parents[child_task_id].append(parent_task_id)

        return wf_by_task_id, children, parents, scheduled_workflow_ids

    def get_root_nodes(self) -> List[TaskNode]:
        return self.root_nodes
