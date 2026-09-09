import uuid
from django.conf import settings
from django.db import models

# Swappable model targets — resolved from settings at class-definition time.
# Override in settings.py to use external models instead of the built-in ones.
_CREDENTIAL_MODEL  = getattr(settings, 'TASK_CREDENTIAL_MODEL',  'workflow_orchestrator.Credential')
_ORGANIZATION_MODEL = getattr(settings, 'TASK_ORGANIZATION_MODEL', 'workflow_orchestrator.Organization')
_RESOURCE_MODEL    = getattr(settings, 'TASK_RESOURCE_MODEL',    'workflow_orchestrator.Resource')
_SERVICE_MODEL     = getattr(settings, 'TASK_SERVICE_MODEL',     'workflow_orchestrator.Service')


# ============================================
# Standalone Credential / Organization / Resource models
# These are used by default.  Point TASK_*_MODEL settings at external models
# to swap them out without changing any task-app code.
# ============================================

class Credential(models.Model):
    """
    Stores authentication data for an API integration.
    Override with TASK_CREDENTIAL_MODEL to use an external credential model.
    """
    name = models.CharField(max_length=200)
    data = models.JSONField(
        default=dict,
        help_text="Auth payload: {'access_token': '...', 'refresh_token': '...', 'expires_at': '...'}"
    )
    limits = models.JSONField(default=dict, blank=True, help_text='API quota limits for this specific credential, e.g., {"quota_rules": [{"type": "daily", "limit": 1000}]}')
    service_id = models.IntegerField(
        null=True, blank=True,
        help_text="Optional service identifier for quota tracking"
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'workflow_orchestrator'
        swappable = 'TASK_CREDENTIAL_MODEL'

    def __str__(self):
        return self.name


class Organization(models.Model):
    """
    Minimal tenant/organization model.
    Override with TASK_ORGANIZATION_MODEL to use an external organization model.
    """
    name = models.CharField(max_length=255, blank=True)
    external_id = models.CharField(
        max_length=255, blank=True,
        help_text="Tenant identifier in the external system (e.g. QuickBooks realmId, Shopify shop domain)"
    )
    metadata = models.JSONField(
        default=dict, blank=True,
        help_text="Arbitrary tenant context available as template variables"
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = 'workflow_orchestrator'
        swappable = 'TASK_ORGANIZATION_MODEL'

    def __str__(self):
        return self.name or str(self.pk)


class Service(models.Model):
    """
    Represents an external SaaS integration (e.g. Shopify, Xero, QuickBooks).
    Holds API-level limits and metadata shared across all its resources.
    """
    name = models.CharField(max_length=200, unique=True)
    limits = models.JSONField(
        default=dict, blank=True,
        help_text=(
            "Service-level quota rules inherited by all resources: "
            "{'quota_rules': [{'type': 'daily', 'limit': 10000, 'scope': 'per_credential'}]}"
        )
    )
    metadata = models.JSONField(default=dict, blank=True)
    client_id = models.CharField(
        max_length=500, blank=True, default='',
        help_text="OAuth2 client_id. Falls back to metadata['client_id'] if blank."
    )
    client_secret = models.CharField(
        max_length=500, blank=True, default='',
        help_text="OAuth2 client_secret. Falls back to metadata['client_secret'] if blank."
    )
    refresh_workflow = models.ForeignKey(
        'Workflow',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='services_refreshed',
        help_text="The workflow executed to refresh credentials for this service."
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = 'workflow_orchestrator'
        swappable = 'TASK_SERVICE_MODEL'

    def __str__(self):
        return self.name


class Resource(models.Model):
    """
    Represents an endpoint or model resource within a Service.

    Override with TASK_RESOURCE_MODEL to use an external resource model.
    """
    name = models.CharField(max_length=200)
    service = models.ForeignKey(
        _SERVICE_MODEL,
        on_delete=models.CASCADE,
        related_name='resources',
        null=True, blank=True,
        help_text="The SaaS integration or AI provider service this resource belongs to"
    )
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        app_label = 'workflow_orchestrator'
        swappable = 'TASK_RESOURCE_MODEL'

    def __str__(self):
        return f"{self.service.name}/{self.name}" if self.service else self.name

# Create your models here.
class Attribute(models.Model):
    name = models.CharField(max_length=100)
    value = models.JSONField()

    def __str__(self):
        return f"{self.name}: {self.value}"
    
class TaskAttribute(models.Model):
    task = models.ForeignKey('Task', on_delete=models.CASCADE, related_name='task_attributes')
    attribute = models.ForeignKey(Attribute, on_delete=models.CASCADE, related_name='task_attributes')
    
    class Meta:
        unique_together = ('task', 'attribute')

    def __str__(self):
        return f"Attribute {self.attribute.id} for Task {self.task.id}"

class WorkflowCredential(models.Model):
    """
    Through model for Workflow-Credential M2M relationship.
    Allows multiple credentials per workflow for quota sharing.
    """
    workflow = models.ForeignKey(
        'Workflow',
        on_delete=models.CASCADE,
        related_name='workflow_credentials'
    )
    credential = models.ForeignKey(
        _CREDENTIAL_MODEL,
        on_delete=models.CASCADE,
        related_name='workflow_credentials'
    )

    # Priority for credential selection
    priority = models.IntegerField(
        default=1,
        help_text="Priority for credential selection (1=highest)"
    )

    is_active = models.BooleanField(
        default=True,
        help_text="False when this credential's token has expired for this workflow and needs reauthorization."
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('workflow', 'credential')
        ordering = ['priority', 'created_at']

    def __str__(self):
        return f"Credential {self.credential_id} for Workflow {self.workflow_id}"


class TaskContinuation(models.Model):
    """
    Through model defining conditional continuation rules for a Task.
    Allows linking a Task to multiple other Tasks with priorities and Jinja2 trigger conditions.
    """
    primary_task = models.ForeignKey('Task', on_delete=models.CASCADE, related_name='continuations')
    continuation_task = models.ForeignKey('Task', on_delete=models.CASCADE, related_name='used_as_continuation')
    priority = models.IntegerField(default=1)
    condition = models.CharField(
        max_length=500,
        help_text="Jinja2 expression. e.g., '{{ result.http_status == 401 }}'"
    )

    class Meta:
        ordering = ['priority']
        unique_together = ('primary_task', 'continuation_task')

    def __str__(self):
        return f"{self.primary_task.slug} -> {self.continuation_task.slug} (priority {self.priority})"


class WorkflowContinuation(models.Model):
    """
    Gatekeeper tasks that execute BEFORE a workflow's main task runs.
    Evaluated at workflow start, whether as a root workflow or as a child triggered via DAG fan-out.
    If a gatekeeper returns ``{"_workflow_action": "skip"}``, the main task is skipped
    but the DAG traversal continues to that workflow's children.
    """
    workflow = models.ForeignKey('Workflow', related_name='pre_continuations', on_delete=models.CASCADE)
    task = models.ForeignKey('Task', related_name='used_as_workflow_pre_continuation', on_delete=models.CASCADE)
    condition = models.CharField(
        max_length=500, blank=True, default="true",
        help_text="Jinja2 expression. e.g., '{{ parent.status == \"success\" }}'"
    )
    priority = models.IntegerField(default=1)

    class Meta:
        ordering = ['priority']
        unique_together = ('workflow', 'task')

    def __str__(self):
        return f"Gatekeeper: {self.task.slug} for Workflow {self.workflow_id}"


class Task(models.Model):
    TASK_TYPE_HTTP             = 'http'
    TASK_TYPE_ORM              = 'orm'
    TASK_TYPE_HUMAN_INPUT      = 'human_input'
    TASK_TYPE_MUTATION         = 'mutation'
    TASK_TYPE_CHECKPOINT       = 'checkpoint'
    TASK_TYPE_PYTHON           = 'python'

    TASK_TYPE_CHOICES = [
        (TASK_TYPE_HTTP,             'HTTP API Call'),
        (TASK_TYPE_ORM,              'Database Operation (ORM)'),
        (TASK_TYPE_HUMAN_INPUT,      'Human Input Required'),
        (TASK_TYPE_MUTATION,         'State Mutation (Logic)'),
        (TASK_TYPE_CHECKPOINT,       'Save Checkpoint & Pause'),
        (TASK_TYPE_PYTHON,           'Python Function'),
    ]



    # Deduplication strategy choices
    DEDUP_BY_ID = 'by_id'
    DEDUP_BY_COMPOSITE = 'by_composite_key'
    DEDUP_BY_HASH = 'by_hash'
    DEDUP_NONE = 'none'

    DEDUP_STRATEGY_CHOICES = [
        (DEDUP_BY_ID, 'By ID Field'),
        (DEDUP_BY_COMPOSITE, 'By Composite Key'),
        (DEDUP_BY_HASH, 'By Content Hash'),
        (DEDUP_NONE, 'No Deduplication'),
    ]

    # Idempotency strategy choices
    IDEMPOTENCY_UPSERT = 'upsert'
    IDEMPOTENCY_KEY = 'idempotency_key'
    IDEMPOTENCY_CHECK_EXISTS = 'check_exists'
    IDEMPOTENCY_NONE = 'none'

    IDEMPOTENCY_STRATEGY_CHOICES = [
        (IDEMPOTENCY_UPSERT, 'Upsert Operation'),
        (IDEMPOTENCY_KEY, 'Idempotency Key'),
        (IDEMPOTENCY_CHECK_EXISTS, 'Check Exists Before Write'),
        (IDEMPOTENCY_NONE, 'No Idempotency'),
    ]

    name = models.CharField(max_length=100)
    slug = models.SlugField(max_length=120, unique=True)  # e.g. "shopify_list_orders"
    description = models.TextField(blank=True, null=True)  # natural language for LLM
    tags = models.JSONField(default=list, blank=True)  # ["shopify", "orders", "read"]
    attributes = models.ManyToManyField(Attribute, related_name='tasks', through=TaskAttribute, through_fields=('task', 'attribute'), blank=True)
    resource = models.ForeignKey(_RESOURCE_MODEL, related_name='tasks', on_delete=models.CASCADE, null=True, blank=True)
    task_type = models.CharField(
        max_length=50,
        choices=TASK_TYPE_CHOICES,
        default=TASK_TYPE_HTTP
    )
    service = models.ForeignKey(
        _SERVICE_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='tasks_direct',
        help_text="Direct service link; required for tasks without a resource."
    )

    @property
    def effective_service(self):
        """Returns self.service, or self.resource.service if service is not set."""
        if self.service_id:
            return self.service
        if self.resource_id and self.resource.service_id:
            return self.resource.service
        return None

    # Deduplication configuration
    deduplication_strategy = models.CharField(
        max_length=50,
        choices=DEDUP_STRATEGY_CHOICES,
        default=DEDUP_NONE,
        help_text="Strategy for deduplicating response items"
    )
    deduplication_config = models.JSONField(
        default=dict,
        blank=True,
        help_text="Dedup config: {key_fields: ['id'], composite_fields: ['org_id', 'external_id']}"
    )

    # Idempotency configuration
    idempotency_strategy = models.CharField(
        max_length=50,
        choices=IDEMPOTENCY_STRATEGY_CHOICES,
        default=IDEMPOTENCY_NONE,
        help_text="Strategy for ensuring idempotent operations"
    )
    idempotency_config = models.JSONField(
        default=dict,
        blank=True,
        help_text="Idempotency config: {unique_fields: ['external_id'], key_template: 'task_{org_id}_{id}'}"
    )

    # Max iterations for continuation (prevent infinite loops)
    max_iterations = models.IntegerField(
        null=True,
        blank=True,
        default=1000,
        help_text="Maximum iterations for continuation functions (pagination limit). Set to null for unlimited iterations."
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']
    
class Workflow(models.Model):
    # Workflow type choices
    TYPE_SYNC = 'sync'
    TYPE_MIGRATION = 'migration'
    TYPE_ANALYTICS = 'analytics'

    WORKFLOW_TYPE_CHOICES = [
        (TYPE_SYNC, 'Data Sync'),
        (TYPE_MIGRATION, 'Data Migration'),
        (TYPE_ANALYTICS, 'Analytics/ETL'),
    ]

    # Template support - allows workflows to be reused across organizations
    is_template = models.BooleanField(
        default=False,
        help_text="If True, this is a reusable template (no organization assignment)"
    )
    template_workflow = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='instances',
        help_text="Reference to template workflow (for instances)"
    )
    template_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Name of the workflow template (for templates only)"
    )
    template_description = models.TextField(
        null=True,
        blank=True,
        help_text="Description of the workflow template (for templates only)"
    )

    organization = models.ForeignKey(
        _ORGANIZATION_MODEL,
        related_name='workflows',
        on_delete=models.CASCADE,
        null=True,  # NULL for templates
        blank=True,
        help_text="Organization that owns this workflow (NULL for templates)"
    )
    task = models.ForeignKey(
        Task,
        related_name='workflows',
        on_delete=models.CASCADE,
        null=False,
        blank=False,
    )
    # Legacy single credential (DEPRECATED - use credentials M2M)
    credential = models.ForeignKey(
        _CREDENTIAL_MODEL,
        related_name='workflows_legacy',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        help_text="DEPRECATED: Use credentials M2M field instead"
    )

    # Multi-credential support (NEW)
    credentials = models.ManyToManyField(
        _CREDENTIAL_MODEL,
        related_name='workflows',
        through='WorkflowCredential',
        through_fields=('workflow', 'credential'),
        blank=False,
        help_text="Credentials sharing quota pool for this workflow"
    )
    schedule = models.ForeignKey(
        'Schedule',
        related_name='workflows',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
    )
    mapped_by = models.ManyToManyField(
        'self',
        symmetrical=False,
        related_name='mapped_to',
        blank=True,
    )

    # Workflow type and metadata
    workflow_type = models.CharField(
        max_length=50,
        choices=WORKFLOW_TYPE_CHOICES,
        default=TYPE_SYNC,
        help_text="Type of workflow: sync (incremental), migration (one-time), or analytics (ETL)"
    )

    # Sync metadata (for sync workflows)
    last_sync_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last successful sync timestamp (for incremental syncs)"
    )

    # Migration metadata (for migration workflows)
    migration_completed = models.BooleanField(
        default=False,
        help_text="Whether migration has completed (for one-time migrations)"
    )
    migration_progress = models.JSONField(
        default=dict,
        blank=True,
        help_text="Migration progress tracking: {records_processed: 0, records_total: 0, last_id: null}"
    )

    # Workflow metadata
    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Additional workflow metadata and configuration"
    )

    # Backfill configuration
    backfill_strategy = models.CharField(
        max_length=50,
        choices=[
            ('full', 'Full Backfill'),
            ('date_range', 'Date Range Backfill'),
            ('optimized', 'Optimized Backfill'),
            ('none', 'No Backfill'),
        ],
        default='none',
        help_text="Strategy for backfilling historical data"
    )

    backfill_config = models.JSONField(
        default=dict,
        blank=True,
        help_text=(
            "Backfill configuration: "
            "{'start_date': '2024-01-01', 'end_date': '2024-12-31', 'batch_days': 30, "
            "'checkpoint_every_n_batches': 10}"
        )
    )

    is_backfill = models.BooleanField(
        default=False,
        help_text="Runtime flag: True if this is a backfill execution (uses low-priority queue)"
    )

    # Priority configuration
    priority_override = models.IntegerField(
        null=True,
        blank=True,
        choices=[(1, 'High'), (2, 'Default'), (3, 'Low')],
        help_text="Override default priority. 1=high, 2=default, 3=low. If not set, uses workflow_type default."
    )

    # Quota configuration
    quota_limit = models.JSONField(
        default=dict,
        blank=True,
        help_text=(
            "Optional per-workflow quota limits to restrict usage. "
            'Example: {"daily_limit": 500, "hourly_limit": 50}. '
            "If not set, workflow uses full credential quota based on priority."
        )
    )

    class Meta:
        ordering = ['organization', 'task']
        indexes = [
            models.Index(fields=['is_template', 'template_name']),
            models.Index(fields=['template_workflow']),
        ]

    def __str__(self):
        if self.is_template:
            return f"Template: {self.template_name or self.task.name}"
        return f"Workflow {self.id}: {self.task.name} for {self.organization}"

    def clean(self):
        """Validate template/instance constraints"""
        from django.core.exceptions import ValidationError

        if self.is_template:
            if self.organization_id is not None:
                raise ValidationError("Template workflows cannot have an organization")
            if not self.template_name:
                raise ValidationError("Template workflows must have a template_name")
        else:
            if self.organization_id is None:
                raise ValidationError("Non-template workflows must have an organization")

    @property
    def rq_queue_name(self) -> str:
        """
        Determine RQ queue based on priority.
        Priority override takes precedence over workflow_type default.
        """
        # Explicit priority override
        if self.priority_override is not None:
            return {1: 'high', 2: 'default', 3: 'low'}[self.priority_override]

        # Default based on workflow type
        return {
            self.TYPE_SYNC: 'high',           # Real-time sync - highest priority
            self.TYPE_MIGRATION: 'default',   # One-time migration - normal priority
            self.TYPE_ANALYTICS: 'low',       # Background ETL - lowest priority
        }.get(self.workflow_type, 'default')

    def get_credentials(self):
        """
        Get all credentials for this workflow.
        Only active credentials via WorkflowCredential are returned.

        Returns:
            QuerySet of Credential objects
        """
        from workflow_orchestrator.conf import get_credential_model
        Credential = get_credential_model()
        active_ids = self.workflow_credentials.filter(is_active=True)\
                         .order_by('priority', 'created_at')\
                         .values_list('credential_id', flat=True)
        return Credential.objects.filter(pk__in=list(active_ids))

    def get_primary_credential(self):
        """
        Get primary credential for this workflow (first by priority).
        Only active credentials via WorkflowCredential are returned.

        Returns:
            Credential instance or None
        """
        wc = self.workflow_credentials.filter(is_active=True)\
                 .select_related('credential')\
                 .order_by('priority', 'created_at')\
                 .first()
        return wc.credential if wc else None

    def get_merged_attributes(self, request_overrides=None):
        """
        Get merged attributes with request overrides taking precedence.

        Precedence order (lowest to highest):
        1. Task attributes (base configuration)
        2. Request overrides (runtime parameters)

        Args:
            request_overrides: Dict of attribute name -> value pairs from the request/checkpoint

        Returns:
            Dict mapping attribute name to value
        """
        # Get task attributes (base configuration)
        task_attrs = {
            ta.attribute.name: ta.attribute.value
            for ta in self.task.task_attributes.select_related('attribute')
        }

        # Merge with request overrides if provided
        if request_overrides:
            return {**task_attrs, **request_overrides}

        return task_attrs

    @property
    def effective_queue_name(self) -> str:
        """
        Determine RQ queue based on priority and backfill mode.
        Backfill executions always use low-priority queue.

        Returns:
            Queue name: 'high', 'default', or 'low'
        """
        # Backfill always goes to low-priority queue
        if self.is_backfill:
            return 'low'

        # Otherwise use existing logic
        return self.rq_queue_name

    @classmethod
    def get_workflows_by_integration(cls, org_id, service_name=None, service_id=None):
        """
        Get workflows for an organization filtered by integration type.

        Args:
            org_id: Organization ID
            service_name: Service name (e.g., 'xero', 'shopify') - optional
            service_id: Service ID - optional

        Returns:
            QuerySet of Workflow objects

        Example:
            # Get all Xero workflows for org 123
            xero_workflows = Workflow.get_workflows_by_integration(123, service_name='xero')

            # Get all workflows for org 123 with service_id=5
            workflows = Workflow.get_workflows_by_integration(123, service_id=5)
        """
        from workflow_orchestrator.conf import get_resource_model
        from django.db.models import Q

        qs = cls.objects.filter(organization_id=org_id)

        if service_name and not service_id:
            Resource = get_resource_model()
            resource = Resource.objects.filter(name__iexact=service_name).first()
            if resource:
                service_id = getattr(resource, 'service_id', None) or resource.pk

        if service_id:
            # Filter by service_id via credential
            # Handle both M2M and FK credential patterns
            qs = qs.filter(
                Q(credentials__service_id=service_id) | Q(credential__service_id=service_id)
            ).distinct()

        return qs.select_related('task', 'organization', 'schedule').prefetch_related(
            'credentials', 'mapped_by'
        )

class Schedule(models.Model):
    tasks = models.ManyToManyField(Task, related_name='schedules', through='Workflow', through_fields=('schedule', 'task'))
    cron_expression = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return f"Schedule: {self.cron_expression}"


class MockTaskResponse(models.Model):
    """
    Stores canned HTTP responses for a Task, organised into named scenarios.

    Each scenario holds an ordered list of responses (the ``responses`` field).
    On the first call that matches this scenario the first item is served, on
    the second call the second item is served, and so on.  Once the list is
    exhausted the last item is repeated indefinitely — which makes it trivial
    to model "first call returns 429, all subsequent calls return 200".

    Matching order (highest-priority match wins):
      1. Explicit ``priority`` field (higher = tried first).
      2. Number of ``match_params`` keys that are present AND equal in the
         actual request — more specific matches beat generic ones.
      3. ``match_method`` (blank = match any method).

    Usage with AiohttpMocker (full patch, no network):
        with AiohttpMocker() as mocker:
            asyncio.run(execute_tasks_in_parallel([task_config]))
        assert mocker.call_count('shopify_list_orders') == 2

    Usage with mock_response_view (partial patch, real Django routing):
        task.set_attribute('url', 'http://testserver/task/mock/shopify_list_orders/')
        response = self.client.post('/task/executor/', ...)
    """

    task = models.ForeignKey(
        'Task',
        on_delete=models.CASCADE,
        related_name='mock_responses',
        help_text="Task this mock response belongs to",
    )
    scenario = models.CharField(
        max_length=100,
        help_text="Human-readable label, e.g. 'success', 'page_2', 'rate_limited_then_ok'",
    )
    description = models.TextField(
        blank=True,
        help_text="What this scenario models",
    )

    # ---- Request matching ------------------------------------------------
    match_method = models.CharField(
        max_length=10,
        blank=True,
        help_text="HTTP method to match (blank = any)",
    )
    match_params = models.JSONField(
        default=dict,
        blank=True,
        help_text=(
            "Query-parameter subset that must be present AND equal in the request. "
            "Empty dict matches any request (lowest specificity). "
            "Example: {\"cursor\": \"abc123\"} only matches page-2 requests."
        ),
    )
    priority = models.IntegerField(
        default=0,
        help_text="Higher value = evaluated before lower-priority scenarios",
    )

    # ---- Response sequence -----------------------------------------------
    responses = models.JSONField(
        default=list,
        help_text=(
            "Ordered list of response dicts served per call. "
            "Format: [{\"status_code\": 200, \"body\": {...}, "
            "\"headers\": {}, \"delay_ms\": 0}]. "
            "Last item is repeated once the list is exhausted."
        ),
    )

    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ('task', 'scenario')
        ordering = ['-priority']

    def __str__(self):
        return f"Mock[{self.task.slug}:{self.scenario}]"

    def get_response_for_call(self, call_index: int) -> dict:
        """Return the response dict for the given (0-based) call index."""
        if not self.responses:
            return {'status_code': 200, 'body': {}, 'headers': {}, 'delay_ms': 0}
        idx = min(call_index, len(self.responses) - 1)
        return self.responses[idx]

    def matches_request(self, method: str, params: dict) -> bool:
        """Return True if this scenario matches the incoming request."""
        if self.match_method and self.match_method.upper() != method.upper():
            return False
        for key, value in self.match_params.items():
            if str(params.get(key)) != str(value):
                return False
        return True

    @property
    def specificity(self) -> int:
        """Number of param keys that must match — used for tie-breaking."""
        return len(self.match_params)


class DataEgressLog(models.Model):
    """
    Track API data egress per request for cost modeling.
    Stores request/response sizes for billing and analytics.
    """
    task_id = models.CharField(
        max_length=200,
        db_index=True,
        help_text="Task execution ID (run_id:workflow_id or run_id:workflow_id:mapping_index)"
    )

    workflow = models.ForeignKey(
        Workflow,
        on_delete=models.CASCADE,
        related_name='egress_logs',
        help_text="Workflow that generated this egress"
    )

    credential = models.ForeignKey(
        _CREDENTIAL_MODEL,
        on_delete=models.CASCADE,
        related_name='egress_logs',
        help_text="Credential used for the API call"
    )

    organization = models.ForeignKey(
        _ORGANIZATION_MODEL,
        on_delete=models.CASCADE,
        related_name='egress_logs',
        help_text="Organization that owns this workflow"
    )

    service = models.ForeignKey(
        _SERVICE_MODEL,
        on_delete=models.CASCADE,
        related_name='egress_logs',
        help_text="Service/integration (Xero, Shopify, etc.)"
    )

    request_size_bytes = models.BigIntegerField(
        default=0,
        help_text="Size of request body in bytes"
    )

    response_size_bytes = models.BigIntegerField(
        default=0,
        help_text="Size of response body in bytes"
    )

    total_size_bytes = models.BigIntegerField(
        default=0,
        help_text="Total egress (request + response) in bytes"
    )

    url = models.TextField(
        blank=True,
        help_text="API endpoint called"
    )

    method = models.CharField(
        max_length=10,
        blank=True,
        help_text="HTTP method (GET, POST, etc.)"
    )

    status_code = models.IntegerField(
        null=True,
        blank=True,
        help_text="HTTP status code"
    )

    timestamp = models.DateTimeField(
        auto_now_add=True,
        db_index=True,
        help_text="When the API call was made"
    )

    run_id = models.CharField(
        max_length=100,
        db_index=True,
        help_text="Workflow run ID for grouping"
    )

    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['credential', 'timestamp']),
            models.Index(fields=['organization', 'timestamp']),
            models.Index(fields=['service', 'timestamp']),
            models.Index(fields=['run_id']),
        ]

    def __str__(self):
        return f"Egress {self.task_id}: {self.total_size_bytes} bytes"

    def save(self, *args, **kwargs):
        # Auto-calculate total
        self.total_size_bytes = self.request_size_bytes + self.response_size_bytes
        super().save(*args, **kwargs)


# ============================================
# Webhook Support Models
# ============================================

class Webhook(models.Model):
    """
    Webhook endpoint configuration for triggering workflows.
    Each webhook has a unique URL and can trigger multiple workflows.
    """
    # Identification
    webhook_id = models.UUIDField(
        default=uuid.uuid4,
        editable=False,
        unique=True,
        db_index=True,
        help_text="Unique identifier for webhook URL generation"
    )

    name = models.CharField(
        max_length=200,
        help_text="Human-readable name (e.g., 'Shopify Order Created')"
    )

    description = models.TextField(
        blank=True,
        null=True,
        help_text="Description of webhook purpose"
    )

    organization = models.ForeignKey(
        _ORGANIZATION_MODEL,
        on_delete=models.CASCADE,
        related_name='webhooks',
        help_text="Organization that owns this webhook"
    )

    # Authentication configuration
    AUTH_HMAC = 'hmac'
    AUTH_BEARER = 'bearer'
    AUTH_BASIC = 'basic'
    AUTH_NONE = 'none'

    AUTH_TYPE_CHOICES = [
        (AUTH_HMAC, 'HMAC Signature'),
        (AUTH_BEARER, 'Bearer Token'),
        (AUTH_BASIC, 'Basic Auth'),
        (AUTH_NONE, 'No Authentication'),
    ]

    auth_type = models.CharField(
        max_length=20,
        choices=AUTH_TYPE_CHOICES,
        default=AUTH_HMAC,
        help_text="Authentication method for webhook verification"
    )

    # Authentication secrets (encrypted in production)
    auth_config = models.JSONField(
        default=dict,
        help_text=(
            "Auth configuration:\n"
            "HMAC: {'secret': 'xxx', 'header_name': 'X-Signature', 'algorithm': 'sha256'}\n"
            "Bearer: {'token': 'xxx', 'header_name': 'Authorization'}\n"
            "Basic: {'username': 'xxx', 'password': 'xxx'}"
        )
    )

    # Workflows to trigger
    workflows = models.ManyToManyField(
        'Workflow',
        related_name='webhooks',
        blank=True,
        help_text="Workflows triggered by this webhook"
    )

    # Webhook configuration
    is_active = models.BooleanField(
        default=True,
        help_text="Whether webhook is active and can receive events"
    )

    # Payload filtering (optional)
    event_filter = models.JSONField(
        default=dict,
        blank=True,
        help_text=(
            "Optional JSONPath filters to conditionally trigger workflows:\n"
            "{'event_type': 'order.created', 'status': 'paid'}"
        )
    )

    # Rate limiting
    rate_limit_per_minute = models.IntegerField(
        default=60,
        help_text="Maximum requests per minute (0 = unlimited)"
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Statistics
    total_requests = models.IntegerField(default=0)
    last_triggered_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['webhook_id']),
            models.Index(fields=['organization', 'is_active']),
        ]

    def __str__(self):
        return f"Webhook: {self.name} ({self.webhook_id})"

    @property
    def webhook_url(self):
        """Generate webhook URL"""
        from django.conf import settings
        base_url = getattr(settings, 'WEBHOOK_BASE_URL', None) or getattr(settings, 'BASE_URL', 'http://localhost:8000')
        return f"{base_url}/task/webhook/{self.webhook_id}/"

    def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify HMAC signature"""
        if self.auth_type != self.AUTH_HMAC:
            return True

        import hmac
        import hashlib

        secret = self.auth_config.get('secret', '').encode('utf-8')
        algorithm = self.auth_config.get('algorithm', 'sha256')

        if algorithm == 'sha256':
            computed = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        elif algorithm == 'sha1':
            computed = hmac.new(secret, payload, hashlib.sha1).hexdigest()
        else:
            raise ValueError(f"Unsupported HMAC algorithm: {algorithm}")

        return hmac.compare_digest(computed, signature)


class WebhookLog(models.Model):
    """
    Log of webhook requests for debugging and monitoring.
    """
    webhook = models.ForeignKey(
        Webhook,
        on_delete=models.CASCADE,
        related_name='logs'
    )

    # Request details
    request_headers = models.JSONField(default=dict)
    request_body = models.TextField()
    request_ip = models.GenericIPAddressField(null=True, blank=True)

    # Response details
    status_code = models.IntegerField()
    response_body = models.TextField(blank=True)

    # Processing details
    triggered_workflows = models.JSONField(
        default=list,
        help_text="List of workflow IDs triggered by this request"
    )

    run_ids = models.JSONField(
        default=list,
        help_text="List of run IDs generated for triggered workflows"
    )

    # Error handling
    error_message = models.TextField(blank=True, null=True)

    # Timestamps
    received_at = models.DateTimeField(auto_now_add=True, db_index=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-received_at']
        indexes = [
            models.Index(fields=['webhook', '-received_at']),
        ]

    def __str__(self):
        return f"WebhookLog {self.id}: {self.webhook.name} at {self.received_at}"


# ============================================
# Checkpoint Model
# ============================================

class Checkpoint(models.Model):
    """
    Stores the state of a paused workflow run.
    """
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE, related_name='checkpoints')
    node_slug = models.CharField(max_length=120)
    context = models.BinaryField()
    reason = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Checkpoint for {self.workflow.id} at node {self.node_slug}"
