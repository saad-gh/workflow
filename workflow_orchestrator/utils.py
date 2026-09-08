import importlib
import logging
import asyncio

logger = logging.getLogger(__name__)


async def async_sleep(ctx=None, **kwargs):
    """Asynchronously sleep for the specified delay in seconds."""
    if isinstance(ctx, dict):
        fn_kwargs = ctx.get('kwargs', {})
        delay = fn_kwargs.get('delay', 5)
    else:
        delay = kwargs.get('delay', 5)

    try:
        delay_float = float(delay)
    except (ValueError, TypeError):
        delay_float = 5.0

    logger.info(f"Async sleep for {delay_float}s")
    await asyncio.sleep(delay_float)
    return {"sleep_delay": delay_float}


async def exponential_backoff_sleep(ctx=None, **kwargs):
    """
    Asynchronously sleep with exponential backoff for retries.
    """
    if isinstance(ctx, dict):
        fn_kwargs = ctx.get('kwargs', {})
        context = ctx.get('context')
        iteration = context.iteration_count if context else 0
    else:
        fn_kwargs = kwargs
        iteration = kwargs.get('iteration', 0)

    try:
        iteration = int(iteration)
    except (ValueError, TypeError):
        iteration = 0

    base_delay = float(fn_kwargs.get('base_delay', 2))
    factor = float(fn_kwargs.get('factor', 2))
    max_delay = float(fn_kwargs.get('max_delay', 60))

    delay = min(base_delay * (factor ** iteration), max_delay)
    logger.info(f"Exponential backoff sleep: iteration={iteration}, delay={delay}s")
    await asyncio.sleep(delay)
    return {"backoff_delay": delay, "iteration": iteration + 1}

def handle_redis_error(exception: Exception) -> None:
    from django.conf import settings
    callback_path = getattr(settings, 'WORKFLOW_REDIS_ERROR_CALLBACK', None)

    if not callback_path:
        logger.error(f"Redis error occurred and no WORKFLOW_REDIS_ERROR_CALLBACK is configured: {exception}")
        return

    try:
        module_path, func_name = callback_path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        fn = getattr(mod, func_name)
        fn(exception)
    except Exception as e:
        logger.error(f"Failed to execute Redis error callback '{callback_path}': {e}", exc_info=True)
