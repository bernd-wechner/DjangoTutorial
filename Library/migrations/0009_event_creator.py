# Generated by Django 2.1.1 on 2019-02-09 11:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Library', '0008_event'),
    ]

    operations = [
        migrations.AddField(
            model_name='event',
            name='creator',
            field=models.CharField(default='', max_length=100),
            preserve_default=False,
        ),
    ]