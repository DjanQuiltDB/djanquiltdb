from django.http import HttpResponse
from django.urls import re_path
from django.views import View


class TestUserView(View):
    def get(self, request):
        return HttpResponse('authenticated' if request.user.is_authenticated else 'anonymous')


# URLs for SessionStoreRequestTestCase
urlpatterns = [re_path(r'^$', TestUserView.as_view(), name='user')]
