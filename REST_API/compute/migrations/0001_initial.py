# -*- coding: utf-8 -*-
# Generated by Django 1.11.5 on 2017-09-13 05:58
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Stat',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataSetUID', models.CharField(max_length=30)),
                ('DB', models.CharField(max_length=30)),
                ('TABLE', models.CharField(max_length=30)),
                ('return_msg', models.CharField(max_length=30)),
            ],
        ),
    ]
