from django.conf import settings
from django.conf.urls import url
from django.http import HttpResponse
from django.test import SimpleTestCase, override_settings
from django.test.client import RequestFactory
from django.views.generic import View

from example.models import Shard
from sharding.middleware import StateExceptionMiddleware
from sharding.tests.utils import ShardingTestCase
from sharding.utils import State, StateException, use_shard, create_template_schema


class StateExceptionTestView(View):
    def get(self, request):
        return HttpResponse('Test exception view')


class TestErrorView(View):
    def get(self, request):
        shard = Shard.objects.get(alias='test_shard')
        with use_shard(shard):
            return HttpResponse("Error should be raised.")


class StateExceptionMiddlewareTestCase(SimpleTestCase):
    def test_process_exception_with_setting(self):
        """
        Case: Call the process_exception of the StateExceptionMiddleware with a view set.
        Expected: The view as response.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'sharding.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            response = StateExceptionMiddleware().process_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertEqual(response.content.decode(), 'Test exception view')

    def test_process_exception_without_setting(self):
        """
        Case: Call the process_exception of the StateExceptionMiddleware, with no view set.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        with override_settings(SHARDING=sharding_settings):
            response = StateExceptionMiddleware().process_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertEqual(response.status_code, 503)

    def test_process_exception_with_invalid_exception(self):
        """
        Case: Call the process_exception of the StateExceptionMiddleware, with the wrong exception.
        Expected: Receive None.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        with override_settings(SHARDING=sharding_settings):
            response = StateExceptionMiddleware().process_exception(
                RequestFactory().get('/'),
                ValueError("test")
            )
        self.assertIsNone(response)


# TestErrorView is not in django's global urls
@override_settings(ROOT_URLCONF=[url(r'^$', TestErrorView.as_view(), name='error')])
class StateExceptionMiddlewareIntegrationTestCase(ShardingTestCase):
    def test_middleware_integration(self):
        """
        Case: Request a view that uses use_shard on a inactive shard.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        create_template_schema('other')
        Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='other',
                             state=State.MAINTENANCE)

        with override_settings(SHARDING=sharding_settings):
            # call the view, that uses use_shard on an nonexistent shard. 503 is raised and caught by the middleware.
            response = self.client.get('/')
        self.assertEqual(response.status_code, 503)
