from django.core.urlresolvers import reverse
from django.test import TestCase

from users.models import Organization, User, Type

from django.contrib.auth import get_user_model


class LoginTestCase(TestCase):
    def setUp(self):
        self.type = Type.objects.create(name='admin')
        self.organization = Organization.objects.create(name='Test Organization')
        self.user = User.objects.create(name='Bob', organization=self.organization, type=self.type, email='bob@net.com')
        self.user.set_password('password')
        self.login_page = reverse("login")

    def test_login(self):
        self.client.login(email=self.user.email, password='password')
