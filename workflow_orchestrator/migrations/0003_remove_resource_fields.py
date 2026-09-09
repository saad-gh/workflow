# Generated for workflow_orchestrator resource cleanup

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('workflow_orchestrator', '0002_remove_ai_models'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='resource',
            name='url',
        ),
        migrations.RemoveField(
            model_name='resource',
            name='limits',
        ),
        migrations.RemoveField(
            model_name='resource',
            name='scopes',
        ),
    ]
