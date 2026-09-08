# Generated manually to strip AI models from workflow_orchestrator

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('workflow_orchestrator', '0001_initial'),
    ]

    operations = [
        migrations.DeleteModel(
            name='AIUsageLog',
        ),
        migrations.DeleteModel(
            name='AITaskGeneration',
        ),
        migrations.DeleteModel(
            name='APIDocumentation',
        ),
    ]
