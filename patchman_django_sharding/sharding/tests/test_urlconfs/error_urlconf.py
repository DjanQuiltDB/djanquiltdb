from django.conf.urls import url

from django.http import HttpResponse
from django.views import View

from example.models import Shard
from sharding.utils import use_shard


class TestErrorView(View):
    def get(self, request):
        shard = Shard.objects.get(alias='test_shard')
        with use_shard(shard):
            return HttpResponse("Error should be raised.")


# New URLs for StateExceptionMiddlewareIntegrationTestCase
urlpatterns = [
    url(r'^$', TestErrorView.as_view(), name='error')
]
