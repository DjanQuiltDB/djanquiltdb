"""
A urlconf that serves the toolkit under /auth/ instead of /o/, for a request on which an earlier middleware set the
``urlconf`` attribute.
"""

from django.urls import include, path

urlpatterns = [
    path('auth/', include('oauth2_provider.urls', namespace='oauth2_provider')),
]
