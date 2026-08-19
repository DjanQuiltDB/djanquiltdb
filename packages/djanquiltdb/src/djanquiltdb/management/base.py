from collections import defaultdict

from django.conf import settings
from django.core.management import CommandError
from django.db import DEFAULT_DB_ALIAS, connections

from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import get_all_databases, get_shard_class, get_template_name, use_shard


def shard_table_exists(node_name=DEFAULT_DB_ALIAS):
    connection_ = connections[node_name]
    all_tables = connection_.get_all_table_headers(schema_name=PUBLIC_SCHEMA_NAME)
    return get_shard_class()._meta.db_table in all_tables


def get_shards_by_node(node_names):
    """
    Read the shard registry once, from the primary connection, and group the selected nodes' shards by the node they
    live on. Reading the registry per node instead hands back every shard once per node that mirrors the registry table.
    """
    primary = settings.QUILT_DB.get('PRIMARY_DB_ALIAS', DEFAULT_DB_ALIAS)
    if not shard_table_exists(primary):
        return {}

    shards_by_node = defaultdict(list)
    with use_shard(node_name=primary, schema_name=PUBLIC_SCHEMA_NAME):
        for shard in get_shard_class().objects.filter(node_name__in=node_names):
            shards_by_node[shard.node_name].append(shard)

    return shards_by_node


def get_databases_and_schema_from_options(options):
    database = options.pop('database', None)
    schema_name = options.pop('schema_name', None)

    # Note that this option is only used internal in the sharding library and cannot be called from the command line.
    check_shard = options.pop('check_shard', True)

    # Get the database we're operating from
    if not database or database == 'all':
        databases = get_all_databases()
    elif database not in get_all_databases():
        raise CommandError('The database you provided does not exist.')
    else:
        databases = [database]

    if schema_name and check_shard and schema_name not in ['public', get_template_name()]:
        if not shard_table_exists():
            raise CommandError(
                'You cannot check whether a shard exists because the public schema does not contain the shard table.'
            )

        for database_ in databases:
            if not get_shard_class().objects.filter(schema_name=schema_name, node_name=database_).exists():
                raise CommandError('Shard {}|{} does not exist.'.format(database_, schema_name))

    return databases, schema_name
