"""
This file is being refactored to use the new OOP architecture.
Many of the old views and functions will be removed or replaced.
"""
import os
import json
import uuid
import logging
logger = logging.getLogger(__name__)
import random
from datetime import datetime, timedelta, timezone as tz
from typing import Any, Dict, List

import jwt
import redis
import requests
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from asgiref.sync import markcoroutinefunction
from .models import Task, Workflow, Schedule
from .orchestrator_core import (
    start_run_for_org,
)

# ============================================
# New Architecture Entry Points
# ============================================

@csrf_exempt
@require_http_methods(["POST"])
async def start_run_view(request):
    """Start a workflow run for an organization"""
    try:
        data = json.loads(request.body) if request.body else {}
        organization_id = data.get("organization_id")
        schedule_id = data.get("schedule_id")
        workflow_id = data.get("workflow_id")

        if not organization_id:
            return JsonResponse({"error": "organization_id is required"}, status=400)

        run_id = await start_run_for_org(organization_id, schedule_id=schedule_id, workflow_id=workflow_id)

        return JsonResponse({
            "run_id": run_id,
            "organization_id": organization_id,
            "schedule_id": schedule_id,
            "status": "started"
        }, status=200)
    except Exception as e:
        logging.error(f"Error in start_run_view: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)


# ============================================
# RQ-Based Workflow Endpoints (To be refactored)
# ============================================

import django_rq

@csrf_exempt
@require_http_methods(["POST"])
def start_workflow_run_rq(request):
    """
    Start a workflow run using RQ background tasks.
    """
    try:
        data = json.loads(request.body) if request.body else {}
        organization_id = data.get("organization_id")
        schedule_id = data.get("schedule_id")

        if not organization_id:
            return JsonResponse({"error": "organization_id is required"}, status=400)

        # Enqueue workflow run to RQ
        queue = django_rq.get_queue('default')
        job = queue.enqueue(
            'workflow_orchestrator.tasks.execute_workflow_run', # Will be updated to a new task
            organization_id=organization_id,
            schedule_id=schedule_id,
            job_timeout='60m'
        )

        return JsonResponse({
            "status": "enqueued",
            "job_id": job.id,
            "organization_id": organization_id,
            "schedule_id": schedule_id,
            "message": "Workflow run has been queued for execution"
        }, status=202)

    except Exception as e:
        logging.error(f"Error in start_workflow_run_rq: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def get_job_status(request):
    """
    Get status of an RQ job.
    """
    try:
        job_id = request.GET.get("job_id")
        if not job_id:
            return JsonResponse({"error": "job_id is required"}, status=400)

        queue = django_rq.get_queue('default')
        job = queue.fetch_job(job_id)

        if not job:
            return JsonResponse({"error": "Job not found"}, status=404)

        response = {
            "job_id": job.id,
            "status": job.get_status(),
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
            "func_name": job.func_name,
        }

        if job.is_finished:
            response["result"] = job.result

        if job.is_failed:
            response["exception"] = str(job.exc_info) if job.exc_info else None

        return JsonResponse(response, status=200)

    except Exception as e:
        logging.error(f"Error in get_job_status: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

# ============================================
# Checkpoint Views
# ============================================

@csrf_exempt
@require_http_methods(["GET"])
def list_checkpoints_view(request):
    try:
        from .models import Checkpoint
        org_id = request.GET.get("organization_id")

        qs = Checkpoint.objects.select_related("workflow", "workflow__task").all()
        if org_id:
            qs = qs.filter(workflow__organization_id=org_id)

        results = []
        for cp in qs:
            results.append({
                "checkpoint_id": cp.id,
                "workflow_id": cp.workflow_id,
                "organization_id": cp.workflow.organization_id,
                "node_slug": cp.node_slug,
                "reason": cp.reason,
                "created_at": cp.created_at.isoformat(),
            })

        return JsonResponse({"checkpoints": results, "count": len(results)}, status=200)
    except Exception as e:
        logging.error(f"Error in list_checkpoints_view: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def resume_checkpoints_view(request):
    try:
        data = json.loads(request.body) if request.body else {}
        checkpoint_ids = data.get("checkpoint_ids", [])

        if not checkpoint_ids:
            return JsonResponse({"error": "checkpoint_ids is required"}, status=400)

        queue = django_rq.get_queue('default')
        resumed = []
        
        for cid in checkpoint_ids:
            job = queue.enqueue(
                'workflow_orchestrator.tasks.resume_workflow_run',
                checkpoint_id=cid,
                job_timeout='60m'
            )
            resumed.append({"checkpoint_id": cid, "job_id": job.id})

        return JsonResponse({"resumed": resumed, "count": len(resumed)}, status=200)
    except Exception as e:
        logging.error(f"Error in resume_checkpoints_view: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)
