from django.core.urlresolvers import reverse
from django.test import TestCase

from example.models import Organization, User, Type, Shard
from sharding.utils import use_shard, State, create_template_schema


class LoginTestCase(TestCase):
    def setUp(self):
        create_template_schema('default')
        self.shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default',
                                          state=State.ACTIVE)
        with use_shard(self.shard):
            self.type = Type.objects.create(name='admin')
            self.organization = Organization.objects.create(name='Test Organization')
            self.user = User.objects.create(name='Bob', organization=self.organization, type=self.type,
                                            email='bob@net.com')
            self.user.set_password('password')
            self.login_page = reverse("login")

    def test_login(self):
        with use_shard(self.shard):
            self.client.login(email=self.user.email, password='password')
