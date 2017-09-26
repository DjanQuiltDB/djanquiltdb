from unittest import mock

from django.conf import settings
from django.conf.urls import url
from django.http import HttpResponse
from django.test import SimpleTestCase, override_settings
from django.test.client import RequestFactory
from django.views.generic import View

from example.models import Shard
from sharding.middleware import BaseUseShardMiddleware, StateExceptionMiddleware
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


class TestNormalView(View):
    def get(self, request):
        return HttpResponse("No error should be raised.")


class UseShardMiddleware(BaseUseShardMiddleware):
    def get_shard_id(self, request):
        return 1


class StateExceptionMiddlewareIntegrationTestCase(ShardingTestCase):
    # TestErrorView is not in django's global urls
    @override_settings(ROOT_URLCONF=[url(r'^$', TestErrorView.as_view(), name='error')])
    @mock.patch('example.middleware.UseShardMiddleware.get_shard_id')
    def test_error_in_view(self, mock_get_shard_id):
        """
        Case: Request a view that contains use_shard on a inactive shard.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        create_template_schema('other')
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='other',
                                     state=State.MAINTENANCE)

        mock_get_shard_id.return_value = shard.id

        with override_settings(SHARDING=sharding_settings):
            # Call the view, that uses use_shard on an nonexistent shard.
            # 503 is raised and caught by the middleware.
            response = self.client.get('/')
        self.assertEqual(response.status_code, 503)
        self.assertTrue(mock_get_shard_id.called)

    # TestErrorView is not in django's global urls
    @override_settings(ROOT_URLCONF=[url(r'^$', TestNormalView.as_view(), name='normal')])
    @mock.patch('example.middleware.UseShardMiddleware.get_shard_id')
    def test_error_in_use_shard(self, mock_get_shard_id):
        """
        Case: Request a view that uses the middleware to get an inactive shard.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        create_template_schema('other')
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='other',
                                     state=State.MAINTENANCE)

        mock_get_shard_id.return_value = shard.id

        with override_settings(SHARDING=sharding_settings):
            # Call the view, the BaseUseShardMiddleware will use use_shard on an nonexistent shard.
            # And the middleware will return 503 instead.
            response = self.client.get('/')
        self.assertEqual(response.status_code, 503)
        self.assertTrue(mock_get_shard_id.called)


class StateExceptionMiddlewareTestCase(SimpleTestCase):
    @mock.patch('sharding.middleware.StateExceptionMiddleware.process_state_exception')
    def test_process_exception_with_state_exception(self, mock_process_state_exception):
        """
        Case: Call the process_exception of the StateExceptionMiddleware with a StateException
        Expected: process_state_exception to be called.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'sharding.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            response = StateExceptionMiddleware().process_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertTrue(mock_process_state_exception.called)

    @mock.patch('sharding.middleware.StateExceptionMiddleware.process_state_exception')
    def test_process_exception_with_other_exception(self, mock_process_state_exception):
        """
        Case: Call the process_exception of the StateExceptionMiddleware with a different exception
        Expected: process_state_exception not to be called.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'sharding.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            StateExceptionMiddleware().process_exception(
                RequestFactory().get('/'),
                ValueError('Generic Error')
            )
        self.assertFalse(mock_process_state_exception.called)

    def test_process_state_exception_with_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware with a view set.
        Expected: The view as response.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'sharding.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            response = UseShardMiddleware().process_state_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertEqual(response.content.decode(), 'Test exception view')

    def test_process_state_exception_without_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware, with no view set.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        with override_settings(SHARDING=sharding_settings):
            response = UseShardMiddleware().process_state_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertEqual(response.status_code, 503)


class BaseUseShardMiddlewareTestCase(SimpleTestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        mock.patch('sharding.middleware.get_shard_class').start()

    @mock.patch('sharding.middleware.use_shard')
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

    @mock.patch('sharding.middleware.use_shard')
    def test_process_view(self, mock_use_shard):
        """
        Case: Call the middleware to process a view.
        Expected: The context manager returned by `use_shard` is
                  entered but not exited.
        """
        mock_use_shard_value = mock.Mock()
        mock_use_shard_value.return_value.enable = mock.Mock()
        mock_use_shard_value.return_value.disable = mock.Mock()
        mock_use_shard.return_value = mock_use_shard_value

        UseShardMiddleware().process_view(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard.called)
        self.assertTrue(mock_use_shard.return_value.enable.called)
        self.assertFalse(mock_use_shard.return_value.disable.called)  # process_response is not called

    @mock.patch('sharding.middleware.use_shard')
    def test_process_view_with_use_shard_exception(self, mock_use_shard):
        """
        Case: Call the middleware to process a shard in maintenance
        Expected: process_state_exception is called,
                  shard_context_manager is not enabled
        """
        mock_use_shard_value = mock.Mock()
        mock_use_shard_value.return_value.enable = mock.Mock()
        mock_use_shard_value.return_value.disable = mock.Mock()
        mock_use_shard.side_effect = \
            StateException("Shard {} state is {}".format(1, State.MAINTENANCE), State.MAINTENANCE)
        mock_use_shard.return_value = mock_use_shard_value

        mock_process_state_exception = \
            mock.patch('sharding.tests.middleware.UseShardMiddleware.process_state_exception').start()

        UseShardMiddleware().process_view(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard.called)
        self.assertFalse(mock_use_shard.return_value.enable.called)
        self.assertFalse(mock_use_shard.return_value.disable.called)
        self.assertTrue(mock_process_state_exception.called)

    def test_process_exception_with_invalid_exception(self):
        """
        Case: Call the process_exception of the UseShardMiddleware,
              with the wrong exception.
        Expected: Context manager is disabled and
                  Process_state_exception not called.
        """
        sharding_settings = settings.SHARDING
        if sharding_settings.get('STATE_EXCEPTION_VIEW', False):
            del sharding_settings['STATE_EXCEPTION_VIEW']

        mock_use_shard_context_manager = \
            mock.patch('sharding.tests.middleware.UseShardMiddleware.shard_context_manager').start()
        mock_return_value = mock.Mock()
        mock_return_value.return_value.disable = mock.Mock()
        mock_use_shard_context_manager.return_value = mock_return_value

        mock_process_state_exception = \
            mock.patch('sharding.tests.middleware.UseShardMiddleware.process_state_exception').start()

        with override_settings(SHARDING=sharding_settings):
            UseShardMiddleware().process_exception(
                RequestFactory().get('/'),
                ValueError("Generic error.")
            )

        self.assertFalse(mock_process_state_exception.called)
        self.assertTrue(mock_use_shard_context_manager.disable.called)
