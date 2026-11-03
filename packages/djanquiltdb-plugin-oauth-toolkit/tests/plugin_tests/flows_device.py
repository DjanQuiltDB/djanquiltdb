from djanquiltdb.utils import use_shard_for
from oauth2_provider.models import get_device_grant_model

from djanquiltdb_plugin_oauth_toolkit.identifiers import parse_client_id, parse_token
from plugin_tests.base import PluginTestCase

DEVICE_CODE_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:device_code'

CLIENT_ID_OF_999 = 'O999I' + 'a' * 40


class DeviceFlowTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        self.application = self.create_application(
            1, client_type='public', authorization_grant_type=DEVICE_CODE_GRANT_TYPE
        )
        self.user = self.create_user(1)

    def authorize_device(self):
        """
        Starts the device flow for the application of organization 1, and returns the device authorization response.
        """
        response = self.post_form('/o/device-authorization/', {'client_id': self.application.client_id})
        self.assertEqual(response.status_code, 200, response.content)

        return response.json()

    def test_the_device_flow_gives_tokens_of_the_shard(self):
        """
        Case: A device of organization 1 starts the device flow, a user of organization 1 enters the user code and
              accepts the device, and the device exchanges the device code for tokens.
        Expected: The device code and the user code have no shard selector, the device grant is stored in the shard of
                  organization 1, the confirmation page has the client id in its path, and the tokens parse to '1'.
        """
        device_authorization = self.authorize_device()

        self.assertIsNone(parse_token(device_authorization['device_code']))
        self.assertIsNone(parse_client_id(device_authorization['user_code']))

        with use_shard_for(1):
            self.assertTrue(
                get_device_grant_model().objects.filter(device_code=device_authorization['device_code']).exists()
            )

        self.log_in(1, self.user)

        response = self.post_form('/o/device/', {'user_code': device_authorization['user_code']})
        self.assertEqual(response.status_code, 302, response.content)

        confirmation_path = response['Location']
        self.assertIn(self.application.client_id, confirmation_path)
        self.assertEqual(self.client.get(confirmation_path).status_code, 200)

        response = self.post_form(confirmation_path, {'action': 'accept'})
        self.assertEqual(response.status_code, 302, response.content)

        self.forget_session()
        response = self.post_form(
            '/o/token/',
            {
                'grant_type': DEVICE_CODE_GRANT_TYPE,
                'device_code': device_authorization['device_code'],
                'client_id': self.application.client_id,
            },
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(parse_token(response.json()['access_token']), '1')

    def test_a_confirmation_page_of_another_organization_is_refused(self):
        """
        Case: A user of organization 2 opens the confirmation page of a device of organization 1.
        Expected: 400 invalid_request, because the client id in the path names another shard than the session.
        """
        device_authorization = self.authorize_device()
        self.log_in(2, self.create_user(2))

        response = self.client.get(
            '/o/device-confirm/{}/{}'.format(self.application.client_id, device_authorization['user_code'])
        )

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')

    def test_a_client_id_of_an_unknown_organization_is_an_invalid_request(self):
        """
        Case: A device starts the device flow with a client id of organization 999, which has no mapping row.
        Expected: 400 invalid_request, as the toolkit answers a client id that it does not know.
        """
        response = self.post_form('/o/device-authorization/', {'client_id': CLIENT_ID_OF_999})

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')
