from django.urls import re_path

from django.http import HttpResponse
from django.views import View


class TestNormalView(View):
    def get(self, request):
        return HttpResponse('No error should be raised.')


# New URLs for StateExceptionMiddlewareIntegrationTestCase
urlpatterns = [
    re_path(r'^$', TestNormalView.as_view(), name='normal')
]
