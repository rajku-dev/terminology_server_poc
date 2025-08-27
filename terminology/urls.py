from django.urls import path
from .views.lookup import get, post
from .views.expand.expand import expand_view
# from .views.expand.expand_cache import expand_view
from .views.validate_code import validate_code_view

urlpatterns = [
    path('CodeSystem/$lookup', get.lookup_get_view),
    path('CodeSystem/$lookup/', post.lookup_post_view),
    path('ValueSet/$expand', expand_view),
    path('CodeSystem/$validate-code', validate_code_view),
]
