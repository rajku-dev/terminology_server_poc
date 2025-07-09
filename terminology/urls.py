from django.urls import path
from .views import lookup_view

urlpatterns = [
    path('CodeSystem/$lookup', lookup_view),
]
