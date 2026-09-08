from django.contrib import admin
from django.db.models import Sum
from .models import Task, Workflow, Schedule, Attribute, TaskAttribute, DataEgressLog, MockTaskResponse


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ['name', 'slug', 'deduplication_strategy', 'idempotency_strategy']
    list_filter = ['deduplication_strategy', 'idempotency_strategy']
    search_fields = ['name', 'slug', 'description']


@admin.register(Workflow)
class WorkflowAdmin(admin.ModelAdmin):
    list_display = [
        'id',
        'task',
        'organization',
        'workflow_type',
        'priority_override',
        'get_queue_name',
        'schedule'
    ]
    list_filter = ['workflow_type', 'priority_override', 'migration_completed']
    search_fields = ['task__name', 'organization__name']
    readonly_fields = ['get_queue_name']

    def get_queue_name(self, obj):
        """Display which RQ queue this workflow will use"""
        return obj.rq_queue_name
    get_queue_name.short_description = 'RQ Queue'

    fieldsets = (
        ('Basic Info', {
            'fields': ('organization', 'task', 'credential', 'schedule')
        }),
        ('Workflow Type & Priority', {
            'fields': ('workflow_type', 'priority_override', 'get_queue_name'),
            'description': 'Priority: Sync=high, Migration=default, Analytics=low. Override to change.'
        }),
        ('Quota Management', {
            'fields': ('quota_limit',),
            'description': 'Optional per-workflow quota restrictions (e.g., {"daily_limit": 500})'
        }),
        ('Sync Metadata', {
            'fields': ('last_sync_timestamp',),
            'classes': ('collapse',)
        }),
        ('Migration Metadata', {
            'fields': ('migration_completed', 'migration_progress'),
            'classes': ('collapse',)
        }),
        ('Other', {
            'fields': ('metadata',),
            'classes': ('collapse',)
        }),
    )


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = ['id', 'cron_expression']
    search_fields = ['cron_expression']


@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'value']
    search_fields = ['name']


@admin.register(TaskAttribute)
class TaskAttributeAdmin(admin.ModelAdmin):
    list_display = ['id', 'task', 'attribute']
    list_filter = ['task']


@admin.register(DataEgressLog)
class DataEgressLogAdmin(admin.ModelAdmin):
    list_display = [
        'task_id',
        'workflow',
        'organization',
        'service',
        'format_total_size',
        'format_request_size',
        'format_response_size',
        'status_code',
        'timestamp'
    ]
    list_filter = [
        'service',
        'timestamp',
        'status_code',
        ('organization', admin.RelatedOnlyFieldListFilter),
        ('credential', admin.RelatedOnlyFieldListFilter),
    ]
    search_fields = ['task_id', 'run_id', 'url']
    readonly_fields = [
        'task_id',
        'workflow',
        'credential',
        'organization',
        'service',
        'request_size_bytes',
        'response_size_bytes',
        'total_size_bytes',
        'url',
        'method',
        'status_code',
        'timestamp',
        'run_id'
    ]
    date_hierarchy = 'timestamp'

    def format_total_size(self, obj):
        """Format total size in human-readable format"""
        return self._format_bytes(obj.total_size_bytes)
    format_total_size.short_description = 'Total Size'

    def format_request_size(self, obj):
        """Format request size in human-readable format"""
        return self._format_bytes(obj.request_size_bytes)
    format_request_size.short_description = 'Request Size'

    def format_response_size(self, obj):
        """Format response size in human-readable format"""
        return self._format_bytes(obj.response_size_bytes)
    format_response_size.short_description = 'Response Size'

    def _format_bytes(self, bytes_value):
        """Convert bytes to human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"

    def changelist_view(self, request, extra_context=None):
        """Add aggregate statistics to changelist view"""
        extra_context = extra_context or {}

        # Calculate totals for current filtered queryset
        queryset = self.get_queryset(request)

        aggregates = queryset.aggregate(
            total_requests=admin.models.Count('id'),
            total_egress=Sum('total_size_bytes'),
            total_request_bytes=Sum('request_size_bytes'),
            total_response_bytes=Sum('response_size_bytes')
        )

        extra_context['total_requests'] = aggregates['total_requests'] or 0
        extra_context['total_egress'] = self._format_bytes(aggregates['total_egress'] or 0)
        extra_context['total_request_bytes'] = self._format_bytes(aggregates['total_request_bytes'] or 0)
        extra_context['total_response_bytes'] = self._format_bytes(aggregates['total_response_bytes'] or 0)

        return super().changelist_view(request, extra_context)


@admin.register(MockTaskResponse)
class MockTaskResponseAdmin(admin.ModelAdmin):
    list_display = ['task', 'scenario', 'match_method', 'priority', 'response_count', 'is_active']
    list_filter = ['is_active', 'match_method', 'task']
    search_fields = ['task__slug', 'task__name', 'scenario', 'description']
    readonly_fields = ['response_count']

    def response_count(self, obj):
        return len(obj.responses)
    response_count.short_description = 'Responses in sequence'
