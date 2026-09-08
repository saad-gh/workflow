"""
Handles structured logging for workflow observability.
"""
import logging
import json

class TelemetryLogger:
    def __init__(self, run_id: str, org_id: int):
        self.run_id = run_id
        self.org_id = org_id
        self.logger = logging.getLogger(__name__)

    def log(self, event_type: str, task_slug: str, **kwargs):
        log_data = {
            "event_type": event_type,
            "run_id": self.run_id,
            "org_id": self.org_id,
            "task_slug": task_slug,
            **kwargs
        }
        self.logger.info(json.dumps(log_data))
