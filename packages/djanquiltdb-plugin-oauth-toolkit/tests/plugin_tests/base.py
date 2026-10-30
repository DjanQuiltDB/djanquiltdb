import base64
import hashlib
import json
import secrets
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from django.conf import settings
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from djanquiltdb import State
from djanquiltdb.sessions import SessionStore
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard_for
from oauth2_provider.models import get_application_model

from example.models import OrganizationShard, Shard, User

#: The raw client secret of every application that create_application() creates. The toolkit stores a hash of it.
CLIENT_SECRET = 'client-secret'

USER_PASSWORD = 'user-password'

REDIRECT_URI = 'https://client.example/callback'

FORM_CONTENT_TYPE = 'application/x-www-form-urlencoded'


def basic_authorization(client_id, client_secret=CLIENT_SECRET):
    """
    Returns the Basic authorization header of a client, with both halves form-encoded as RFC 6749 asks.
    """
    credentials = '{}:{}'.format(quote_plus(client_id), quote_plus(client_secret))

    return 'Basic ' + base64.b64encode(credentials.encode()).decode()


def unverified_claims(id_token):
    """
    Returns the claims of a signed JWT, without checking its signature.
    """
    payload = id_token.split('.')[1]

    return json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))


class PluginTestCase(ShardingTransactionTestCase):
    """
    Base case for the tests that create shards for real. ``available_apps`` restricts the migration graph, so it lists
    every app of the project.

    setUp creates the shard first on the primary node, the shard remote on the other node, and the shard maintained on
    the primary node, in maintenance. Organization 1 maps onto first, organization 2 onto remote and organization 3 onto
    maintained.
    """

    available_apps = [
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'djanquiltdb',
        'djanquiltdb_plugin_oauth_toolkit',
        'oauth2_provider',
        'example',
    ]

    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.remote = Shard.objects.create(
            alias='remote', node_name='other', schema_name='remote_schema', state=State.ACTIVE
        )
        self.maintained = Shard.objects.create(
            alias='maintained', node_name='default', schema_name='maintained_schema', state=State.ACTIVE
        )

        OrganizationShard.objects.create(shard=self.first, organization_id=1)
        OrganizationShard.objects.create(shard=self.remote, organization_id=2)
        OrganizationShard.objects.create(shard=self.maintained, organization_id=3)

    def put_shard_in_maintenance(self, shard):
        """
        Puts ``shard`` in maintenance until the end of the test. The flush after the test enters every shard, so the
        shard is made active again before then.
        """
        shard.state = State.MAINTENANCE
        shard.save(update_fields=['state'])
        self.addCleanup(self.make_shard_active, shard)

    @staticmethod
    def make_shard_active(shard):
        shard.state = State.ACTIVE
        shard.save(update_fields=['state'])

    @staticmethod
    def create_user(organization_id, email='user@example.com'):
        with use_shard_for(organization_id):
            return User.objects.create_user(email=email, password=USER_PASSWORD)

    @staticmethod
    def create_application(organization_id, **fields):
        """
        Creates an application in the shard of ``organization_id``. Its client secret is CLIENT_SECRET.
        """
        fields = {
            'name': 'Client of organization {}'.format(organization_id),
            'client_type': 'confidential',
            'authorization_grant_type': 'client-credentials',
            'client_secret': CLIENT_SECRET,
            **fields,
        }

        with use_shard_for(organization_id):
            return get_application_model().objects.create(**fields)

    def log_in(self, organization_id, user):
        """
        Logs ``user`` in on the test client, with a session of the session backend of DjanQuiltDB, whose key has the
        shard selector ``organization_id``. The test client's own login() makes a session key without a shard selector.
        """
        session = SessionStore(shard_selector=organization_id)
        session[SESSION_KEY] = str(user.pk)
        session[BACKEND_SESSION_KEY] = 'django.contrib.auth.backends.ModelBackend'
        session[HASH_SESSION_KEY] = user.get_session_auth_hash()
        session.save()

        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

    def forget_session(self):
        """
        Removes the session cookie from the test client, so that the next requests have no session. The test client's
        own logout() looks up the user outside any shard.
        """
        self.client.cookies.pop(settings.SESSION_COOKIE_NAME, None)

    def post_form(self, path, data, **headers):
        """
        Posts ``data`` to ``path`` as a form-encoded body, as OAuth clients send it.
        """
        return self.client.post(path, urlencode(data), content_type=FORM_CONTENT_TYPE, headers=headers)

    def request_client_credentials_token(self, application, scope='read'):
        """
        Returns the token response of the client credentials grant for ``application``, which authenticates with
        Basic authentication.
        """
        response = self.post_form(
            '/o/token/',
            {'grant_type': 'client_credentials', 'scope': scope},
            Authorization=basic_authorization(application.client_id),
        )
        self.assertEqual(response.status_code, 200, response.content)

        return response.json()

    def request_authorization_code_tokens(self, organization_id, application, user, scope='openid read'):
        """
        Runs the authorization code grant with PKCE for ``user`` of ``organization_id``: the user logs in, allows
        ``application`` on the authorization page, and the application exchanges the code for tokens. Returns the token
        response.
        """
        code_verifier = secrets.token_urlsafe(48)
        code_challenge = base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).decode().rstrip('=')
        authorization_parameters = {
            'client_id': application.client_id,
            'response_type': 'code',
            'redirect_uri': REDIRECT_URI,
            'scope': scope,
            'code_challenge': code_challenge,
            'code_challenge_method': 'S256',
        }

        self.log_in(organization_id, user)

        response = self.client.get('/o/authorize/', authorization_parameters)
        self.assertEqual(response.status_code, 200, response.content)

        response = self.post_form('/o/authorize/', {**authorization_parameters, 'allow': 'Authorize'})
        self.assertEqual(response.status_code, 302, response.content)
        code = parse_qs(urlparse(response['Location']).query)['code'][0]

        response = self.post_form(
            '/o/token/',
            {
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': REDIRECT_URI,
                'code_verifier': code_verifier,
            },
            Authorization=basic_authorization(application.client_id),
        )
        self.assertEqual(response.status_code, 200, response.content)

        return response.json()

    def create_authorization_code_application(self, organization_id):
        return self.create_application(
            organization_id,
            authorization_grant_type='authorization-code',
            redirect_uris=REDIRECT_URI,
            algorithm='RS256',
        )

    def request_protected_resource(self, token):
        """
        Returns the response of the protected resource of the project for a Bearer ``token``.
        """
        return self.client.get('/resource/', headers={'Authorization': 'Bearer ' + token})
