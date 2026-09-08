"""
RQ exception handlers and utilities.
"""
import logging

logger = logging.getLogger(__name__)


def log_exception_handler(job, exc_type, exc_value, traceback):
    """
    Exception handler for RQ jobs.
    Logs exceptions and can implement custom error handling logic.
    """
    logger.error(
        f"RQ Job {job.id} failed: {exc_type.__name__}: {exc_value}",
        exc_info=(exc_type, exc_value, traceback),
        extra={
            'job_id': job.id,
            'job_func': job.func_name,
            'job_args': job.args,
            'job_kwargs': job.kwargs,
        }
    )

    # TODO: Add custom error handling
    # - Send alerts for critical failures
    # - Update workflow status in database
    # - Push to dead letter queue if max retries exceeded

    return True  # Return False to re-raise exception
