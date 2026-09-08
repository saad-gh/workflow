"""
Helper utilities for connecting extraction workflows to BigQuery loading workflows.

Users of workflow_orchestrator can import and use these functions as signal receivers,
or write their own custom receivers following this pattern.
"""
import json
import logging

logger = logging.getLogger(__name__)


def bq_load_after_extract(sender, org_id, run_id, schedule_id, workflow_id_map=None, **kwargs):
    """
    Signal receiver that triggers a BigQuery load workflow after an extraction workflow completes.

    Connects to ``workflow_orchestrator.signals.run_completed``.

    Args:
        org_id: Organization ID
        run_id: Completed run ID
        schedule_id: Schedule ID
        workflow_id_map: Dict mapping extract task slugs to (bq_load_workflow_id, table_name) tuples.
            Example: {
                'xero_extract_invoices': (10, 'xero_invoices'),
                'xero_extract_journals': (11, 'xero_journals'),
            }
    """
    if not workflow_id_map:
        return

    from .views import get_async_redis
    from .orchestrator_core import start_run_for_org_with_payload

    rdb = get_async_redis()

    async def _collect_and_trigger():
        pattern = f"task_result:{org_id}:*:{run_id}:*"
        extract_slugs = list(workflow_id_map.keys())

        results_by_slug = {}
        async for key in rdb.scan_iter(pattern):
            val = await rdb.get(key)
            if not val:
                continue
            doc = json.loads(val)
            slug = doc.get("task_slug")
            status = doc.get("status")
            if slug in extract_slugs and status == "success":
                results_by_slug.setdefault(slug, []).append(doc)

        for slug, docs in results_by_slug.items():
            bq_workflow_id, table_name = workflow_id_map[slug]

            aggregated_data = []
            for doc in docs:
                response_json = doc.get("response_json") or {}
                items_key = None
                for k in ("Invoices", "Journals", "ManualJournals", "items", "data", "results"):
                    if k in response_json:
                        items_key = k
                        break
                if items_key:
                    for item in response_json[items_key]:
                        aggregated_data.append({
                            "id": item.get("InvoiceID") or item.get("JournalID") or item.get("ManualJournalID") or item.get("id") or str(hash(json.dumps(item, sort_keys=True))),
                            "raw_payload": item,
                        })

            if aggregated_data:
                await start_run_for_org_with_payload(
                    org_id, bq_workflow_id,
                    {"table_name": table_name, "data": aggregated_data},
                    rdb
                )

    try:
        import asyncio
        asyncio.run(_collect_and_trigger())
    except Exception as e:
        logger.error(f"Error in bq_load_after_extract signal receiver: {e}", exc_info=True)
