# Generated for workflow_orchestrator Service api_spec_path field

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('workflow_orchestrator', '0003_remove_resource_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='service',
            name='api_spec_path',
            field=models.CharField(
                blank=True,
                default='',
                help_text='Path or URI to standardized API specification document (e.g., specs/shopify-2024-01.yaml or gs://bucket/spec.yaml)',
                max_length=500
            ),
        ),
    ]
