from django.http import HttpResponse
from django.urls import include, path
from django.views import View


def sync_function_view(request):
    return HttpResponse('sync')


async def async_function_view(request):
    return HttpResponse('async')


class SyncClassView(View):
    def get(self, request):
        return HttpResponse('sync')


class AsyncClassView(View):
    async def get(self, request):
        return HttpResponse('async')


included_patterns = [
    path('async/', async_function_view),
    path('sync/', sync_function_view),
]

# URLs for AsyncViewsCheckTestCase
urlpatterns = [
    path('sync-function/', sync_function_view),
    path('async-function/', async_function_view),
    path('sync-class/', SyncClassView.as_view()),
    path('async-class/', AsyncClassView.as_view()),
    path('included/', include(included_patterns)),
]
