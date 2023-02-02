from django.conf.urls import url

from django.http import HttpResponse
from django.views import View


class TestNormalView(View):
    def get(self, request):
        return HttpResponse('No error should be raised.')


# New URLs for StateExceptionMiddlewareIntegrationTestCase
urlpatterns = [
    url(r'^$', TestNormalView.as_view(), name='normal')
]
