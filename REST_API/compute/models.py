from django.db import models
from rest_framework import  serializers

# Create your models here.

class Stat(models.Model):
    dataSetUID = models.CharField(max_length=30)
    DB = models.CharField(max_length=30)
    TABLE = models.CharField(max_length=30)
    return_msg = models.CharField(max_length=30)