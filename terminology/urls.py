from django.urls import path
from .views.lookup import lookup_view
from .views.lookup_post import lookup_post_view
# from .views.validate_code import validate_code_view
# from .views.expand import expand_view

urlpatterns = [
    path('CodeSystem/$lookup', lookup_view),
    path('CodeSystem/$lookup/', lookup_post_view),
    # path('CodeSystem/$expand', expand_view),
    # path('CodeSystem/$validate-code', validate_code_view),
]
