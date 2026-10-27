import functools
import inspect

from django.urls import path

from djanquiltdb_tests.test_urlconfs.async_urlconf import async_function_view


class AsyncViewInstance:
    async def __call__(self, request):
        return await async_function_view(request)

    def __repr__(self):
        return '<async view instance>'


# URLs for AsyncViewsCheckTestCase.test_an_async_view_wrapped_in_a_partial
urlpatterns = [
    path('partial/', functools.partial(async_function_view)),
    path('instance/', inspect.markcoroutinefunction(AsyncViewInstance())),
]
