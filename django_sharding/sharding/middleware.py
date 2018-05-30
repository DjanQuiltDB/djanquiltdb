import logging

from django.conf import settings
from django.http import HttpResponse
from django.utils.module_loading import import_string

from sharding.utils import StateException, use_shard, get_shard_class, use_shard_for


logger = logging.getLogger(__name__)


class StateExceptionMiddleware(object):
    def process_exception(self, request, exception):
        if not isinstance(exception, StateException):
            return None
        return self.process_state_exception(request, exception)

    def process_state_exception(self, request, exception):
        if settings.SHARDING.get('STATE_EXCEPTION_VIEW', None):  # call custom view
            return import_string(settings.SHARDING['STATE_EXCEPTION_VIEW']).as_view()(request)
        else:  # no view set, return error
            response = HttpResponse()
            response.status_code = 503
            return response


class BaseUseShardMiddleware(StateExceptionMiddleware):
    def process_request(self, request):
        self.set_shard_context_manager(request, None)

        if not hasattr(self, 'get_mapping_value') and not hasattr(self, 'get_shard_id'):
            raise NotImplementedError(
                'The `BaseUseShardMiddleware` middleware class requires that one of `get_shard_id` and '
                '`get_mapping_value` is implemented.'
            )

        try:
            request._mapping_value = getattr(self, 'get_mapping_value', lambda *args, **kwargs: None)(request)
            request._shard_id = getattr(self, 'get_shard_id', lambda *args, **kwargs: None)(request)
            if request._mapping_value:
                self._enable_shard_for(request, request._mapping_value)
            elif request._shard_id:
                self._enable_shard(request, request._shard_id)
            else:
                logger.debug('No shard selected in `BaseUseShardMiddleware`', extra={'request': request})
        except StateException as exception:
            return self.process_exception(request, exception)

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

    def _enable_shard(self, request, shard_id):
        shard = get_shard_class().objects.get(id=shard_id)
        shard_context_manager = self.set_shard_context_manager(request, use_shard(shard))
        shard_context_manager.enable()

    def _enable_shard_for(self, request, target_value):
        shard_context_manager = self.set_shard_context_manager(request, use_shard_for(target_value))
        shard_context_manager.enable()

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


try:
    # noinspection PyUnresolvedReferences
    # https://docs.djangoproject.com/en/1.11/topics/http/middleware/#upgrading-pre-django-1-10-style-middleware
    from django.utils.deprecation import MiddlewareMixin
except ImportError:
    pass
else:
    # noinspection PyAbstractClass
    class BaseUseShardMiddleware(MiddlewareMixin, BaseUseShardMiddleware):  # nosec
        pass
