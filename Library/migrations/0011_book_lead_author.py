# Generated by Django 2.1.7 on 2019-02-16 06:01

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('Library', '0010_auto_20190215_2325'),
    ]

    operations = [
        migrations.AddField(
            model_name='book',
            name='lead_author',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to='Library.Author'),
            preserve_default=False,
        ),
    ]
