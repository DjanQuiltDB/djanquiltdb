from braces.views import LoginRequiredMixin
from django.views.generic import TemplateView

from users.models import User


class HomeView(LoginRequiredMixin, TemplateView):
    template_name = 'users/home.html'

    def get_context_data(self, **kwargs):
        kwargs['users'] = User.objects.all()
        return super().get_context_data(**kwargs)