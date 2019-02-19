from django.shortcuts import render
from rest_framework import viewsets
from compute.serializers import StatSerializer
from compute.models import Stat

# Create your views here.

class StatViewSet(viewsets.ModelViewSet):
    queryset = Stat.objects.all()
    serializer_class = StatSerializer
