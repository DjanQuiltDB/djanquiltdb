from unittest import mock

from django.conf import settings
from django.conf.urls import url
from django.http import HttpResponse
from django.test import SimpleTestCase, override_settings
from django.test.client import RequestFactory
from django.views.generic import View

from example.models import Shard
from sharding.middleware import StateExceptionMiddleware, BaseUseShardMiddleware
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


class UseShardMiddleware(BaseUseShardMiddleware):
    def get_shard_id(self, request):
        return 1


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


@mock.patch('sharding.middleware.use_shard')
class BaseUseShardMiddlewareTestCase(SimpleTestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        mock.patch('sharding.middleware.get_shard_class').start()

    def test_process_view(self, mock_utils_use_shard):
        """
        Case: Call the middleware to process a view.
        Expected: The context manager returned by `use_shard` is
                  entered but not exited.
        """
        mock_use_shard = mock.Mock()
        mock_use_shard.return_value.enable = mock.Mock()
        mock_use_shard.return_value.disable = mock.Mock()

        mock_utils_use_shard.return_value = mock_use_shard

        UseShardMiddleware().process_view(RequestFactory().get('/'))

        mock_use_shard.enable.assert_called_with()
        self.assertFalse(mock_use_shard.disable.called)  # called by `process_view`

    def test_process_response(self, mock_utils_use_shard):
        """
        Case: Call the middleware to process a response.
        Expected: The context manager returned by `use_shard` is
                  exited.
        """
        mock_use_shard = mock.Mock()
        mock_use_shard.return_value.enable = mock.Mock()
        mock_use_shard.return_value.disable = mock.Mock()

        mock_utils_use_shard.return_value = mock_use_shard

        request, response = RequestFactory().get('/'), HttpResponse()
        middleware = UseShardMiddleware()

        middleware.process_view(request)  # required, sets the context manager
        middleware.process_response(request, response)

        mock_use_shard.enable.assert_called_with()  # called by `process_view`
        mock_use_shard.disable.assert_called_with()  # called by `process_response`

    def test_process_exception(self, mock_utils_use_shard):
        """
        Case: Call the middleware to process an exception.
        Expected: The context manager returned by `use_shard` is
                  exited.
        """
        mock_use_shard = mock.Mock()
        mock_use_shard.return_value.enable = mock.Mock()
        mock_use_shard.return_value.disable = mock.Mock()

        mock_utils_use_shard.return_value = mock_use_shard

        exc = ValueError('test')
        request = RequestFactory().get('/')
        middleware = UseShardMiddleware()

        middleware.process_view(request)  # required, sets the context manager
        middleware.process_exception(request, exc)

        mock_use_shard.enable.assert_called_with()  # called by `process_view`
        mock_use_shard.disable.assert_called_with()  # called by `process_exception`
