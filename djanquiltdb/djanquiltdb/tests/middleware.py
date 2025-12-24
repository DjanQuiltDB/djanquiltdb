from unittest import mock

from django.conf import settings
from django.db import OperationalError
from django.http import HttpResponse
from django.test import SimpleTestCase, override_settings
from django.test.client import RequestFactory
from django.views.generic import View, TemplateView

from example.models import Shard
from djanquiltdb.db import connection
from djanquiltdb.middleware import BaseUseShardMiddleware, BaseUseShardForMiddleware, ExceptionMiddlewareMixin
from djanquiltdb.tests import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import State, StateException, create_template_schema


class StateExceptionTestView(View):
    def get(self, request):
        return HttpResponse('Test state exception view')


class StateExceptionTestTemplateView(TemplateView):
    template_name = 'example/state_exception.html'


class ConnectionExceptionTestView(View):
    def get(self, request):
        return HttpResponse('Test connection exception view')


class ConnectionExceptionTestTemplateView(TemplateView):
    template_name = 'example/connection_exception.html'


class UseShardMiddleware(BaseUseShardMiddleware):
    def get_shard_id(self, request):
        return 1


class UseShardForMiddleware(BaseUseShardForMiddleware):
    def get_mapping_value(self, request):
        return 1


class ExceptionMiddlewareMixinIntegrationTestCase(ShardingTransactionTestCase):
    # TestErrorView is not in django's global urls
    @override_settings(ROOT_URLCONF='djanquiltdb.tests.test_urlconfs.error_urlconf')
    @mock.patch('example.middleware.UseShardMiddleware.get_shard_id')
    def test_state_error_in_view(self, mock_get_shard_id):
        """
        Case: Request a view that contains use_shard on a inactive shard.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('STATE_EXCEPTION_VIEW', False)

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

    # TestNormalView is not in django's global urls
    @override_settings(ROOT_URLCONF='djanquiltdb.tests.test_urlconfs.normal_urlconf')
    @mock.patch('example.middleware.UseShardMiddleware.get_shard_id')
    def test_state_error_in_use_shard(self, mock_get_shard_id):
        """
        Case: Request a view that uses the middleware to get an inactive shard.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('STATE_EXCEPTION_VIEW', False)

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

    @override_settings(ROOT_URLCONF='djanquiltdb.tests.test_urlconfs.error_urlconf')
    @mock.patch('example.middleware.UseShardMiddleware.get_shard_id')
    def test_connection_error(self, mock_get_shard_id):
        """
        Case: Request a view that raises a connection error for accessing a node that is down.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('CONNECTION_EXCEPTION_VIEW', False)

        create_template_schema('other')
        shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='other',
                                     state=State.ACTIVE)

        mock_get_shard_id.return_value = shard.id

        with override_settings(SHARDING=sharding_settings):
            with mock.patch('psycopg.connect', side_effect=OperationalError):
                # Close the connection. With `connect` being mocked no new connection can be started,
                # making the node effectively unavailable
                connection.close()
                # Call the view, that uses use_shard on an nonexistent shard.
                # 503 is raised and caught by the middleware.
                response = self.client.get('/')

        self.assertEqual(response.status_code, 503)
        self.assertTrue(mock_get_shard_id.called)


class ExceptionMiddlewareMixinTestCase(SimpleTestCase):
    @mock.patch('djanquiltdb.middleware.ConnectionExceptionProcessor.process_exception')
    @mock.patch('djanquiltdb.middleware.StateExceptionProcessor.process_exception')
    def test_process_exception_with_state_exception(self, mock_state_process_exception,
                                                    mock_connection_process_exception):
        """
        Case: Call the process_exception of the ExceptionMiddlewareMixin with a StateException.
        Expected: process_exception to be called on StateExceptionProcessor.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            ExceptionMiddlewareMixin(lambda x: None).process_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertTrue(mock_state_process_exception.called)
        mock_connection_process_exception.assert_not_called()

    @mock.patch('djanquiltdb.middleware.ConnectionExceptionProcessor.process_exception')
    @mock.patch('djanquiltdb.middleware.StateExceptionProcessor.process_exception')
    def test_process_exception_with_connection_exception(self, mock_state_process_exception,
                                                         mock_connection_process_exception):
        """
        Case: Call the process_exception of the ExceptionMiddlewareMixin with a OperationalError.
        Expected: process_exception to be called on ConnectionExceptionProcessor.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            ExceptionMiddlewareMixin(lambda x: None).process_exception(
                RequestFactory().get('/'),
                OperationalError('Node is not available')
            )
        mock_state_process_exception.assert_not_called()
        self.assertTrue(mock_connection_process_exception.called)

    @mock.patch('djanquiltdb.middleware.ConnectionExceptionProcessor.process_exception')
    @mock.patch('djanquiltdb.middleware.StateExceptionProcessor.process_exception')
    def test_process_exception_with_other_exception(self, mock_state_process_exception,
                                                    mock_connection_process_exception):
        """
        Case: Call the process_exception of the StateOrConnectionExceptionMiddleware with a different exception
        Expected: No process_exception to be called.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            ExceptionMiddlewareMixin(lambda x: None).process_exception(
                RequestFactory().get('/'),
                ValueError('Generic Error')
            )
        self.assertFalse(mock_state_process_exception.called)
        self.assertFalse(mock_connection_process_exception.called)

    def test_state_process_exception_with_view_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware with a view set.
        Expected: The view as response, no render function called for it.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.StateExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            with self.subTest('Test renderer call'):
                with mock.patch('django.template.response.TemplateResponse.render') as mock_render:
                    UseShardMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        StateException('Shard is not in available state!', 'M')
                    )
                self.assertFalse(mock_render.called)

            with self.subTest('Test response'):
                response = UseShardMiddleware(lambda x: None).process_exception(
                    RequestFactory().get('/'),
                    StateException('Shard is not in available state!', 'M')
                )
                self.assertContains(response, 'Test state exception view', html=True)

    def test_state_process_exception_with_templateview_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware with a TemplateView set.
        Expected: The view is rendered and return as response.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['STATE_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.StateExceptionTestTemplateView'

        with override_settings(SHARDING=sharding_settings):
            with self.subTest('Test renderer call'):
                with mock.patch('django.template.response.TemplateResponse.render') as mock_render:
                    UseShardMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        StateException('Shard is not in available state!', 'M')
                    )
                self.assertTrue(mock_render.called)

            with self.subTest('Test response'):
                response = UseShardMiddleware(lambda x: None).process_exception(
                    RequestFactory().get('/'),
                    StateException('Shard is not in available state!', 'M')
                )
                self.assertContains(response, '<h2>Shard unavailable</h2>', html=True)

    def test_state_process_exception_without_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware, with no view set.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('STATE_EXCEPTION_VIEW', False)

        with override_settings(SHARDING=sharding_settings):
            response = UseShardMiddleware(lambda x: None).process_exception(
                RequestFactory().get('/'),
                StateException('Shard is not in available state!', 'M')
            )
        self.assertEqual(response.status_code, 503)

    def test_connection_process_exception_with_view_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware with a view set.
        Expected: The view as response, no render function called for it.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['CONNECTION_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.ConnectionExceptionTestView'

        with override_settings(SHARDING=sharding_settings):
            with self.subTest('Test renderer call'):
                with mock.patch('django.template.response.TemplateResponse.render') as mock_render:
                    UseShardMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        OperationalError('Node is not available')
                    )
                self.assertFalse(mock_render.called)

            with self.subTest('Test response'):
                response = UseShardMiddleware(lambda x: None).process_exception(
                    RequestFactory().get('/'),
                    OperationalError('Node is not available')
                )
                self.assertContains(response, 'Test connection exception view', html=True)

    def test_connection_process_exception_with_templateview_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware with a TemplateView set.
        Expected: The view is rendered and return as response.
        """
        sharding_settings = settings.SHARDING
        sharding_settings['CONNECTION_EXCEPTION_VIEW'] = 'djanquiltdb.tests.middleware.ConnectionExceptionTestTemplateView'

        with override_settings(SHARDING=sharding_settings):
            with self.subTest('Test renderer call'):
                with mock.patch('django.template.response.TemplateResponse.render') as mock_render:
                    UseShardMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        OperationalError('Node is not available')
                    )
                self.assertTrue(mock_render.called)

            with self.subTest('Test response'):
                response = UseShardMiddleware(lambda x: None).process_exception(
                    RequestFactory().get('/'),
                    OperationalError('Node is not available')
                )
                self.assertContains(response, '<h2>Database unavailable</h2>', html=True)

    def test_connection_process_exception_without_setting(self):
        """
        Case: Call the process_exception of the UseShardMiddleware, with no view set.
        Expected: 503 status received.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('CONNECTION_EXCEPTION_VIEW', False)

        with override_settings(SHARDING=sharding_settings):
            response = UseShardMiddleware(lambda x: None).process_exception(
                RequestFactory().get('/'),
                OperationalError('Node is not available')
            )
        self.assertEqual(response.status_code, 503)


class BaseUseShardMiddlewareTestCase(ShardingTestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        mock.patch('djanquiltdb.middleware.get_shard_class').start()

    @mock.patch('djanquiltdb.middleware.use_shard')
    def test_process_response(self, mock_utils_use_shard):
        """
        Case: Call the middleware to process a response.
        Expected: The context manager returned by `use_shard` is exited.
        """
        mock_use_shard = mock.Mock()
        mock_use_shard.return_value.enable = mock.Mock()
        mock_use_shard.return_value.disable = mock.Mock()

        mock_utils_use_shard.return_value = mock_use_shard

        request, response = RequestFactory().get('/'), HttpResponse()
        middleware = UseShardMiddleware(lambda x: None)

        middleware.process_request(request)  # required, sets the context manager
        middleware.process_response(request, response)

        mock_use_shard.enable.assert_called_with()  # called by `process_view`
        mock_use_shard.disable.assert_called_with()  # called by `process_response`

    @mock.patch('djanquiltdb.middleware.use_shard')
    def test_process_request(self, mock_use_shard):
        """
        Case: Call the middleware to process a request.
        Expected: The context manager returned by `use_shard` is entered but not exited.
        """
        mock_use_shard_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_value.enable = enable_mock
        mock_use_shard_value.disable = disable_mock
        mock_use_shard.return_value = mock_use_shard_value

        request = RequestFactory().get('/')
        UseShardMiddleware(lambda x: None).process_request(request)

        self.assertTrue(mock_use_shard.called)
        self.assertTrue(enable_mock.called)
        self.assertFalse(disable_mock.called)  # process_response is not called
        self.assertEqual(request._shard_id, 1)

    @mock.patch('djanquiltdb.middleware.use_shard')
    def test_process_request_with_use_shard_exception(self, mock_use_shard):
        """
        Case: Call the middleware to process a shard in maintenance.
        Expected: process_exception is called, shard_context_manager is not enabled.
        """
        mock_use_shard_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_value.enable = enable_mock
        mock_use_shard_value.disable = disable_mock
        mock_use_shard.return_value = mock_use_shard_value
        mock_use_shard.side_effect = \
            StateException('Shard {} state is {}'.format(1, State.MAINTENANCE), State.MAINTENANCE)

        with mock.patch('djanquiltdb.middleware.ExceptionProcessor.process_exception') as mock_process_exception:
            UseShardMiddleware(lambda x: None).process_request(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard.called)
        self.assertFalse(enable_mock.called)
        self.assertFalse(disable_mock.called)
        self.assertTrue(mock_process_exception.called)

    @mock.patch('djanquiltdb.middleware.use_shard')
    def test_process_request_with_connection_exception(self, mock_use_shard):
        """
        Case: Call the middleware to process a node that is down.
        Expected: process_exception is called, shard_context_manager is not enabled.
        """
        mock_use_shard_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_value.enable = enable_mock
        mock_use_shard_value.disable = disable_mock
        mock_use_shard.side_effect = OperationalError('Could not set up connection')
        mock_use_shard.return_value = mock_use_shard_value

        with mock.patch('djanquiltdb.middleware.ExceptionProcessor.process_exception') as mock_process_exception:
            UseShardMiddleware(lambda x: None).process_request(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard.called)
        self.assertFalse(enable_mock.called)
        self.assertFalse(disable_mock.called)
        self.assertTrue(mock_process_exception.called)

    def test_process_exception_with_invalid_exception(self):
        """
        Case: Call the process_exception of the UseShardMiddleware, with the wrong exception.
        Expected: Context manager is disabled and Process_state_exception not called.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('STATE_EXCEPTION_VIEW', False)

        with mock.patch('djanquiltdb.tests.middleware.UseShardMiddleware.get_shard_context_manager') as \
                mock_use_shard_context_manager:
            mock_return_value = mock.Mock()
            mock_return_value.return_value.disable = mock.Mock()
            mock_use_shard_context_manager.return_value = mock_return_value

            with mock.patch('djanquiltdb.middleware.ExceptionMiddlewareMixin.process_exception') as mock_process_exception:
                with override_settings(SHARDING=sharding_settings):
                    UseShardMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        ValueError('Generic error.')
                    )

        self.assertFalse(mock_process_exception.called)
        self.assertTrue(mock_use_shard_context_manager.return_value.disable.called)

    @mock.patch('djanquiltdb.middleware.use_shard')
    def test_shard_context_manager(self, mock_use_shard):
        """
        Case: Test keeping the shard context manager state on the request.
        Expected: shard context manager has been saved on the request, in a class context to be able to use
                  BaseUseShardMiddleware for multiple middleware.
        """
        mock_use_shard_value = mock.Mock()
        mock_use_shard_value.return_value.enable = mock.Mock()
        mock_use_shard.return_value = mock_use_shard_value

        request = RequestFactory().get('/')

        middleware = UseShardMiddleware(lambda x: None)
        middleware.process_request(request)

        self.assertEqual(request._middleware_shard_context_manager[UseShardMiddleware], mock_use_shard_value)

        class SecondUseShardMiddleware(UseShardMiddleware):
            pass

        middleware = SecondUseShardMiddleware(lambda x: None)
        middleware.process_request(request)

        self.assertEqual(request._middleware_shard_context_manager, {
            UseShardMiddleware: mock_use_shard_value,
            SecondUseShardMiddleware: mock_use_shard_value,
        })


class BaseUseShardForMiddlewareTestCase(ShardingTestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        mock.patch('djanquiltdb.middleware.get_shard_class').start()

    @mock.patch('djanquiltdb.middleware.use_shard_for')
    def test_process_response(self, mock_use_shard_for):
        """
        Case: Call the middleware to process a response.
        Expected: The context manager returned by `use_shard` is exited.
        """
        mock_use_shard_for_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_for_value.enable = enable_mock
        mock_use_shard_for_value.disable = disable_mock
        mock_use_shard_for.return_value = mock_use_shard_for_value

        request, response = RequestFactory().get('/'), HttpResponse()
        middleware = UseShardForMiddleware(lambda x: None)

        middleware.process_request(request)  # Required, sets the context manager
        middleware.process_response(request, response)

        enable_mock.assert_called_with()  # Called by `process_view`
        disable_mock.assert_called_with()  # Called by `process_response`

    @mock.patch('djanquiltdb.middleware.use_shard_for')
    def test_process_request(self, mock_use_shard_for):
        """
        Case: Call the middleware to process a request.
        Expected: The context manager returned by `use_shard_for` is entered but not exited.
        """
        mock_use_shard_for_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_for_value.enable = enable_mock
        mock_use_shard_for_value.disable = disable_mock
        mock_use_shard_for.return_value = mock_use_shard_for_value

        request = RequestFactory().get('/')
        UseShardForMiddleware(lambda x: None).process_request(request)

        self.assertTrue(mock_use_shard_for.called)
        self.assertTrue(enable_mock.called)
        self.assertFalse(disable_mock.called)  # process_response is not called
        self.assertEqual(request._mapping_value, 1)

    @mock.patch('djanquiltdb.middleware.use_shard_for')
    def test_process_request_with_use_shard_exception(self, mock_use_shard_for):
        """
        Case: Call the middleware to process a shard in maintenance.
        Expected: process_exception is called, shard_context_manager is not enabled.
        """
        mock_use_shard_for_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_for_value.enable = enable_mock
        mock_use_shard_for_value.disable = disable_mock
        mock_use_shard_for.side_effect = \
            StateException('Shard {} state is {}'.format(1, State.MAINTENANCE), State.MAINTENANCE)
        mock_use_shard_for.return_value = mock_use_shard_for_value

        with mock.patch('djanquiltdb.middleware.BaseUseShardForMiddleware.process_exception') as mock_process_exception:
            UseShardForMiddleware(lambda x: None).process_request(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard_for.called)
        self.assertFalse(enable_mock.called)
        self.assertFalse(disable_mock.called)
        self.assertTrue(mock_process_exception.called)

    @mock.patch('djanquiltdb.middleware.use_shard_for')
    def test_process_request_with_connection_exception(self, mock_use_shard_for):
        """
        Case: Call the middleware to process a node that is down.
        Expected: process_exception is called, shard_context_manager is not enabled.
        """
        mock_use_shard_for_value = mock.Mock()
        enable_mock = mock.Mock()
        disable_mock = mock.Mock()
        mock_use_shard_for_value.enable = enable_mock
        mock_use_shard_for_value.disable = disable_mock
        mock_use_shard_for.side_effect = OperationalError('Could not set up connection')
        mock_use_shard_for.return_value = mock_use_shard_for_value

        with mock.patch('djanquiltdb.middleware.BaseUseShardForMiddleware.process_exception') as mock_process_exception:
            UseShardForMiddleware(lambda x: None).process_request(RequestFactory().get('/'))

        self.assertTrue(mock_use_shard_for.called)
        self.assertFalse(enable_mock.called)
        self.assertFalse(disable_mock.called)
        self.assertTrue(mock_process_exception.called)

    def test_process_exception_with_invalid_exception(self):
        """
        Case: Call the process_exception of the UseShardForMiddleware, with the wrong exception.
        Expected: Context manager is disabled and process_exception not called.
        """
        sharding_settings = settings.SHARDING
        sharding_settings.pop('STATE_EXCEPTION_VIEW', False)

        with mock.patch('djanquiltdb.tests.middleware.UseShardForMiddleware.get_shard_context_manager') as \
                mock_use_shard_for_context_manager:

            mock_return_value = mock.Mock()
            mock_return_value.return_value.disable = mock.Mock()
            mock_use_shard_for_context_manager.return_value = mock_return_value

            with mock.patch('djanquiltdb.middleware.ExceptionMiddlewareMixin.process_exception') as mock_process_exception:
                with override_settings(SHARDING=sharding_settings):
                    UseShardForMiddleware(lambda x: None).process_exception(
                        RequestFactory().get('/'),
                        ValueError('Generic error.')
                    )

        self.assertFalse(mock_process_exception.called)
        self.assertTrue(mock_use_shard_for_context_manager.return_value.disable.called)
