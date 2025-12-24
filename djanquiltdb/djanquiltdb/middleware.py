import logging

from django.conf import settings
from django.db import OperationalError
from django.http import HttpResponse
from django.utils.deprecation import MiddlewareMixin
from django.utils.module_loading import import_string

from djanquiltdb.utils import StateException, get_shard_class, use_shard, use_shard_for

logger = logging.getLogger(__name__)


class ExceptionProcessor(object):
    exception = NotImplementedError('ExceptionMiddlewareMixin must have `exception` defined on the implementing class')
    view_setting = NotImplementedError(
        'ExceptionMiddlewareMixin must have `view_setting` defined on the implementing class'
    )
    status_code = NotImplementedError(
        'ExceptionMiddlewareMixin must have `status_code` defined on the implementingclass'
    )

    @classmethod
    def process_exception(cls, request, exception):
        if settings.SHARDING.get(cls.view_setting, None):  # call custom view
            response = import_string(settings.SHARDING[cls.view_setting]).as_view()(request)
            # If we get a TemplateView that is not yet rendered, call for that here. Otherwise, just pass it.
            if getattr(response, 'is_rendered', True):
                return response
            return response.render()
        else:  # No view set, return error
            response = HttpResponse()
            response.status_code = cls.status_code
            return response


class StateExceptionProcessor(ExceptionProcessor):
    view_setting = 'STATE_EXCEPTION_VIEW'
    exception = StateException
    status_code = 503


class ConnectionExceptionProcessor(ExceptionProcessor):
    view_setting = 'CONNECTION_EXCEPTION_VIEW'
    exception = OperationalError
    status_code = 503


class ExceptionMiddlewareMixin(object):
    processors = (StateExceptionProcessor, ConnectionExceptionProcessor)

    def process_exception(self, request, exception):
        for processor in self.processors:
            if isinstance(exception, processor.exception):
                return processor.process_exception(request, exception)

        return None


class _BaseShardMiddleware(ExceptionMiddlewareMixin, object):
    def process_exception(self, request, exception):
        self._disable_shard(request)
        return super().process_exception(request, exception)

    def process_response(self, request, response):
        self._disable_shard(request)
        return response

    def _disable_shard(self, request):
        shard_context_manager = self.get_shard_context_manager(request)
        if shard_context_manager:
            shard_context_manager.disable()
            self.set_shard_context_manager(request, None)

    def get_shard_context_manager(self, request):
        """
        We cannot properly keep state on a middleware, because it will be shared among multiple requests. Therefore we
        keep the state on the request. Since it can happen that BaseUseShardMiddleware will be used in multiple
        middleware classes, we make sure we add the class name so that the middleware knows which shard context manager
        it has to pick.
        """
        if not hasattr(request, '_middleware_shard_context_manager'):
            return None

        return request._middleware_shard_context_manager.get(self.__class__)

    def set_shard_context_manager(self, request, value):
        if not hasattr(request, '_middleware_shard_context_manager'):
            request._middleware_shard_context_manager = {}

        request._middleware_shard_context_manager[self.__class__] = value
        return request._middleware_shard_context_manager[self.__class__]


class BaseUseShardMiddleware(_BaseShardMiddleware):
    def get_shard_id(self, request):
        raise NotImplementedError(
            'The `BaseUseShardMiddleware` middleware class requires that `get_shard_id` is implemented.'
        )

    def process_request(self, request):
        self.set_shard_context_manager(request, None)

        try:
            request._shard_id = self.get_shard_id(request)
            if request._shard_id:
                self._enable_shard(request, request._shard_id)
        except (StateException, OperationalError) as exception:
            return self.process_exception(request, exception)

    def _enable_shard(self, request, shard_id):
        shard = get_shard_class().objects.get(id=shard_id)
        shard_context_manager = self.set_shard_context_manager(request, use_shard(shard))
        shard_context_manager.enable()


class BaseUseShardForMiddleware(_BaseShardMiddleware):
    def get_mapping_value(self, request):
        raise NotImplementedError(
            'The `BaseUseShardForMiddleware` middleware class requires that `get_mapping_value` is implemented.'
        )

    def process_request(self, request):
        self.set_shard_context_manager(request, None)

        try:
            request._mapping_value = self.get_mapping_value(request)
            if request._mapping_value:
                self._enable_shard_for(request, request._mapping_value)
        except (StateException, OperationalError) as exception:
            return self.process_exception(request, exception)

    def _enable_shard_for(self, request, target_value):
        shard_context_manager = self.set_shard_context_manager(request, use_shard_for(target_value))
        shard_context_manager.enable()


# noinspection PyAbstractClass
class ExceptionMiddlewareMixin(MiddlewareMixin, ExceptionMiddlewareMixin):  # nosec
    pass


# noinspection PyAbstractClass
class BaseUseShardMiddleware(MiddlewareMixin, BaseUseShardMiddleware):  # nosec
    pass


# noinspection PyAbstractClass
class BaseUseShardForMiddleware(MiddlewareMixin, BaseUseShardForMiddleware):  # nosec
    pass


class UseShardMiddleware(BaseUseShardMiddleware, ExceptionMiddlewareMixin):
    """
    Default UseShardMiddleware compatible with djanquiltdb.sessions backend.
    """

    def get_shard_id(self, request):
        return getattr(request.session, settings.SHARDING.get('SESSION_SHARD_SELECTOR_KEY', 'shard_selector'))


class UseShardForMiddleware(BaseUseShardForMiddleware, ExceptionMiddlewareMixin):
    """
    Default UseShardForMiddleware compatible with djanquiltdb.sessions backend.
    """

    def get_mapping_value(self, request):
        return getattr(request.session, settings.SHARDING.get('SESSION_SHARD_SELECTOR_KEY', 'shard_selector'))
