from django.http import JsonResponse
from django.views import View
from djanquiltdb.router import get_active_connection
from oauth2_provider.views.generic import ProtectedResourceView


def active_connection_description():
    active_connection = get_active_connection()

    return {
        'node': getattr(active_connection, 'node_name', active_connection),
        'schema': getattr(active_connection, 'schema_name', None),
    }


class ProtectedResourceEchoView(ProtectedResourceView):
    """
    A resource that django-oauth-toolkit protects. It returns the node and the schema that the request runs in, and the
    email address of the user that the token belongs to.
    """

    def get(self, request, *args, **kwargs):
        resource_owner = request.resource_owner

        return JsonResponse(
            {
                **active_connection_description(),
                'resource_owner': None if resource_owner is None else resource_owner.email,
            }
        )


class RequestUserEchoView(View):
    """
    Returns the node and the schema that the request runs in, and the email address of request.user. The toolkit's
    OAuth2TokenMiddleware sets request.user from a Bearer token.
    """

    def get(self, request, *args, **kwargs):
        user = request.user

        return JsonResponse({**active_connection_description(), 'user': user.email if user.is_authenticated else None})
