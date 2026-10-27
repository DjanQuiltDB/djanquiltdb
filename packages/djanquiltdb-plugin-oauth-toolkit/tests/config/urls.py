from django.urls import include, path

from example.views import ProtectedResourceEchoView, RequestUserEchoView

urlpatterns = [
    path('o/', include('oauth2_provider.urls', namespace='oauth2_provider')),
    path('resource/', ProtectedResourceEchoView.as_view()),
    path('whoami/', RequestUserEchoView.as_view()),
]
