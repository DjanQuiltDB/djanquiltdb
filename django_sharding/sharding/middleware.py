from django.conf import settings
from django.http import HttpResponse
from django.utils.module_loading import import_string

from sharding.utils import StateException


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
