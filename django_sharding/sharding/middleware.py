from django.conf import settings
from django.http import HttpResponse
from django.utils.module_loading import import_string

from sharding.utils import StateException, use_shard, get_shard_class, use_shard_for


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
    shard_context_manager = None

    def get_shard_id(self, request):
        raise NotImplementedError(
            'The `BaseUseShardMiddleware` middleware class requires that `get_shard_id` is implemented.'
        )

    def process_request(self, request):
        try:
            request._shard_id = self.get_shard_id(request)
            if request._shard_id:
                self._enable_shard(request._shard_id)
        except StateException as exception:
            return self.process_exception(request, exception)

    def process_exception(self, request, exception):
        if self.shard_context_manager:
            self.shard_context_manager.disable()
        return super().process_exception(request, exception)

    def process_response(self, request, response):
        if self.shard_context_manager:
            self.shard_context_manager.disable()
        return response

    def _enable_shard(self, shard_id):
        shard = get_shard_class().objects.get(id=shard_id)
        self.shard_context_manager = use_shard(shard)
        self.shard_context_manager.enable()

    def _enable_shard_for(self, target_value):
        self.shard_context_manager = use_shard_for(target_value)
        self.shard_context_manager.enable()


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
