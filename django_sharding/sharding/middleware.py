import django
from django.conf import settings
from django.http import HttpResponse
from django.utils.module_loading import import_string

from sharding.utils import StateException, use_shard, get_shard_class


class StateExceptionMiddleware(object):
    def process_exception(self, request, exception):
        if not isinstance(exception, StateException):
            return None
        if settings.SHARDING.get('STATE_EXCEPTION_VIEW', None):  # call custom view
            return import_string(settings.SHARDING['STATE_EXCEPTION_VIEW']).as_view()(request)
        else:  # no view set, return error
            response = HttpResponse()
            response.status_code = 503
            return response


class BaseUseShardMiddleware(object):
    shard_context_manager = None

    def get_shard_id(self, request):
        raise NotImplementedError(
            'The `BaseUseShardMiddleware` middleware class requires that `get_shard_id` is implemented.'
        )

    def process_view(self, request, *args, **kwargs):
        shard_id = self.get_shard_id(request)

        if shard_id:
            shard = get_shard_class().objects.get(id=shard_id)

            self.shard_context_manager = use_shard(shard)
            self.shard_context_manager.__enter__()

    def process_exception(self, request, exception):
        if self.shard_context_manager:
            self.shard_context_manager.__exit__(
                exc_type=exception.__class__,
                exc_value=exception,
                exc_traceback=exception.__traceback__,
            )

    def process_response(self, request, response):
        if self.shard_context_manager:
            self.shard_context_manager.__exit__(None, None, None)

        return response


# With Django 1.10 we can use the "MIDDLEWARE" setting and a new
# middleware format to write use the context manager without
# manually calling enter and exit.
if django.VERSION[:1] >= (1, 10):
    class BaseUseShardMiddleware(object):
        def __init__(self, get_response):
            self.get_response = get_response

        def __call__(self, request):
            shard_id = self.get_shard_id(request)

            if shard_id:
                shard = get_shard_class().objects.get(id=shard_id)

                with use_shard(shard):
                    return self.get_response(request)

            return self.get_response(request)

        def get_shard_id(self, request):
            raise NotImplementedError(
                'The `BaseUseShardMiddleware` middleware class requires that `get_shard_id` is implemented.'
            )
