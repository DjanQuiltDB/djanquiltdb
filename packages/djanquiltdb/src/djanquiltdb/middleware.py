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
        if settings.QUILT_DB.get(cls.view_setting, None):  # call custom view
            response = import_string(settings.QUILT_DB[cls.view_setting]).as_view()(request)
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
        """
        The primary key of the shard this request belongs to, or a falsy value to leave the request unsharded.

        Abstract: implement it on your subclass. A common source is the session, written there by the login flow.
        """
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
        """
        The mapping value this request belongs to, or a falsy value to leave the request unsharded. The shard is
        then looked up through the ``MAPPING_MODEL``.

        Abstract: implement it on your subclass.
        """
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
    """
    Turns an unreachable shard into a response instead of a traceback.

    A ``StateException`` or an ``OperationalError`` raised while processing a view becomes a 503, or the view named
    by the ``STATE_EXCEPTION_VIEW`` and ``CONNECTION_EXCEPTION_VIEW`` settings when either is set.
    """

    pass


# noinspection PyAbstractClass
class BaseUseShardMiddleware(MiddlewareMixin, BaseUseShardMiddleware):  # nosec
    """
    Wraps each request in ``use_shard``, so views need not know the project is sharded.

    Abstract: subclass it and implement :meth:`get_shard_id`. The shard is released again when the response is
    returned, and an unreachable one is handled as :class:`ExceptionMiddlewareMixin` describes.
    """

    pass


# noinspection PyAbstractClass
class BaseUseShardForMiddleware(MiddlewareMixin, BaseUseShardForMiddleware):  # nosec
    """
    :class:`BaseUseShardMiddleware` for a project with a mapping model: the request names a mapping value rather
    than a shard.

    Abstract: subclass it and implement :meth:`get_mapping_value`.
    """

    pass


class UseShardMiddleware(BaseUseShardMiddleware, ExceptionMiddlewareMixin):
    """
    Default UseShardMiddleware compatible with djanquiltdb.sessions backend.
    """

    def get_shard_id(self, request):
        """The shard id the session carries, under the key ``SESSION_SHARD_SELECTOR_KEY`` names."""
        return getattr(request.session, settings.QUILT_DB.get('SESSION_SHARD_SELECTOR_KEY', 'shard_selector'))


class UseShardForMiddleware(BaseUseShardForMiddleware, ExceptionMiddlewareMixin):
    """
    Default UseShardForMiddleware compatible with djanquiltdb.sessions backend.
    """

    def get_mapping_value(self, request):
        """The mapping value the session carries, under the key ``SESSION_SHARD_SELECTOR_KEY`` names."""
        return getattr(request.session, settings.QUILT_DB.get('SESSION_SHARD_SELECTOR_KEY', 'shard_selector'))
