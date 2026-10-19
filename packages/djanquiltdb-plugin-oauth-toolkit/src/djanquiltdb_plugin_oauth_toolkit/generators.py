"""
The ``OAUTH2_PROVIDER`` settings point django-oauth-toolkit at the client id generator and the token generators of this
module. Each generator returns an identifier with the shard selector of the active shard. Therefore, an application has
to be created inside the shard of its tenant, and a token has to be issued inside that shard too.
"""

from djanquiltdb_plugin_oauth_toolkit.identifiers import (
    generate_client_id_for_selector,
    generate_token_for_selector,
    parse_client_id,
)


class ShardSelectorError(RuntimeError):
    """
    Raised when no shard selector can be found for a new identifier, and when an identifier names another shard than
    the active shard.
    """

    pass


def active_shard_selector():
    """
    Returns the shard selector of the active shard as a string. For a shard that ``use_shard_for()`` entered, it is the
    mapping value that ``use_shard_for()`` was given. For a shard that ``use_shard()`` entered, it is the mapping value
    of the only mapping row of the shard.

    Raises ShardSelectorError outside a shard, in the public schema of a node, in a project without a ``MAPPING_MODEL``,
    and in a shard that has no mapping row or more than one.
    """
    from djanquiltdb.options import ShardOptions
    from djanquiltdb.router import get_active_connection
    from djanquiltdb.utils import get_mapping_class

    active_connection = get_active_connection()
    hint = 'Enter the shard of the tenant with use_shard_for(<mapping value>).'

    if not isinstance(active_connection, ShardOptions) or active_connection.is_public_schema():
        raise ShardSelectorError(
            'The active connection {} is not a shard, so there is no shard selector. {}'.format(active_connection, hint)
        )

    if active_connection.mapping_value is not None:
        return str(active_connection.mapping_value)

    mapping_model = get_mapping_class()

    if mapping_model is None:
        raise ShardSelectorError(
            "The shard selector is a mapping value, and QUILT_DB has no MAPPING_MODEL. Set QUILT_DB['MAPPING_MODEL']."
        )

    mapping_values = list(
        mapping_model.objects.filter(shard_id=active_connection.shard_id).values_list(
            mapping_model.mapping_field, flat=True
        )
    )

    if len(mapping_values) != 1:
        raise ShardSelectorError(
            'The shard {}|{} has {} mapping rows, so its shard selector is unclear. {}'.format(
                active_connection.node_name, active_connection.schema_name, len(mapping_values), hint
            )
        )

    return str(mapping_values[0])


class ShardSelectorClientIdGenerator:
    """
    The client id generator for ``OAUTH2_PROVIDER['CLIENT_ID_GENERATOR_CLASS']``, which DjanQuiltDB serves as
    ``djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator``. When an application is instantiated without a
    client id, the toolkit calls :meth:`hash` without arguments, so an application has to be instantiated inside the
    shard of its tenant.

    The toolkit only calls :meth:`hash`, so this class does not subclass the toolkit's ``BaseHashGenerator``. Importing
    ``oauth2_provider`` reads the Django settings, and DjanQuiltDB can import the plugin before Django is configured.
    """

    def hash(self):
        return generate_client_id_for_selector(active_shard_selector())


def _token_for_client_of_request(request):
    selector = active_shard_selector()
    client = getattr(request, 'client', None)

    if client is None:
        raise ShardSelectorError('The token request has no client, so the shard of its token is unknown.')

    client_selector = parse_client_id(client.client_id)

    if client_selector != selector:
        raise ShardSelectorError(
            'The client id {!r} has the shard selector {!r}, and the active shard has the shard selector {!r}. A token '
            'for this client would name another shard than the client.'.format(
                client.client_id, client_selector, selector
            )
        )

    return generate_token_for_selector(selector)


def generate_access_token(request):
    """
    The access token generator for ``OAUTH2_PROVIDER['ACCESS_TOKEN_GENERATOR']``. The toolkit calls it with the
    oauthlib request, whose client is the application that the token is for.
    """
    return _token_for_client_of_request(request)


def generate_refresh_token(request):
    """
    The refresh token generator for ``OAUTH2_PROVIDER['REFRESH_TOKEN_GENERATOR']``. The toolkit calls it with the
    oauthlib request, whose client is the application that the token is for.
    """
    return _token_for_client_of_request(request)


def refuse_foreign_client_id(sender, instance, raw=False, **kwargs):
    """
    Refuses to save an application whose client id does not have the shard selector of the active shard. Such a client
    id can be set by hand, in the admin or with ``createapplication --client-id``, and no request could name its shard.

    This function is a ``pre_save`` receiver of the toolkit's application model. It does nothing for a raw save, such
    as one from loaddata.
    """
    if raw:
        return

    selector = active_shard_selector()

    if parse_client_id(instance.client_id) != selector:
        raise ShardSelectorError(
            'The client id {!r} does not have the shard selector {!r} of the active shard. Leave the client id empty, '
            'so that the toolkit generates one.'.format(instance.client_id, selector)
        )
