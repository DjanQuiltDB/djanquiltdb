from braces.views import LoginRequiredMixin
from django.views.generic import TemplateView

from example.models import User


class HomeView(LoginRequiredMixin, TemplateView):
    template_name = 'users/home.html'

    def get_context_data(self, **kwargs):
        kwargs['example'] = User.objects.all()
        return super().get_context_data(**kwargs)