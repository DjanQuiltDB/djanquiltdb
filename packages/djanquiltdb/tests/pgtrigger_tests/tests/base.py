"""
Shared harness for the django-pgtrigger suite: a base case and the catalog queries the assertions read.

Every helper names the schema explicitly in the query rather than leaning on the search path, so a test can inspect
the template, the public schema and a shard from the same connection without switching it.
"""

from django.db import connections

from djanquiltdb.testing import ShardingTransactionTestCase
from pgtrigger_tests.models import PublicProtected, ShardedProtected

# The function pgtrigger renders into every trigger to decide whether `pgtrigger.ignore` is in effect. Its name is
# hard-coded to the public schema by pgtrigger itself (compiler.UpsertTriggerSql defaults ignore_func_name to
# '"public"._pgtrigger_should_ignore'), which is what makes it shared by every schema rather than cloned per shard.
IGNORE_FUNC_NAME = '_pgtrigger_should_ignore'


def triggers_in_schema(schema_name, node_name='default'):
    """Return ``{(relation, trigger)}`` for every non-internal trigger in ``schema_name``."""
    cursor = connections[node_name].cursor()
    cursor.execute(
        'SELECT cls.relname::text, tg.tgname::text '
        'FROM pg_catalog.pg_trigger tg '
        'JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid '
        'JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid '
        'WHERE nsp.nspname = %s AND NOT tg.tgisinternal',
        [schema_name],
    )
    return set(cursor.fetchall())


def trigger_definition(schema_name, relation, trigger, node_name='default'):
    """Return the ``pg_get_triggerdef`` text of one trigger, or None when it is not there."""
    cursor = connections[node_name].cursor()
    cursor.execute(
        'SELECT pg_get_triggerdef(tg.oid) '
        'FROM pg_catalog.pg_trigger tg '
        'JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid '
        'JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid '
        'WHERE nsp.nspname = %s AND cls.relname = %s AND tg.tgname = %s AND NOT tg.tgisinternal',
        [schema_name, relation, trigger],
    )
    row = cursor.fetchone()
    return row[0] if row else None


def functions_in_schema(schema_name, node_name='default'):
    """Return the names of every function in ``schema_name``."""
    cursor = connections[node_name].cursor()
    cursor.execute(
        'SELECT pro.proname::text '
        'FROM pg_catalog.pg_proc pro '
        'JOIN pg_catalog.pg_namespace nsp ON pro.pronamespace = nsp.oid '
        'WHERE nsp.nspname = %s',
        [schema_name],
    )
    return {name for (name,) in cursor.fetchall()}


class PgtriggerTestCase(ShardingTransactionTestCase):
    """
    Base case for the pgtrigger suite, on the transactional harness because these tests create shards for real.

    ``available_apps`` restricts the migration graph, and the base class limits it to this library and ``example``,
    which would drop the models carrying the triggers under test. ``example`` stays because ``SHARD_CLASS`` points at
    its Shard model.
    """

    available_apps = ['djanquiltdb', 'example', 'pgtrigger_tests']

    @property
    def sharded_table(self):
        return ShardedProtected._meta.db_table

    @property
    def sharded_trigger(self):
        """The name Postgres knows the trigger by, which is pgtrigger's hashed pgid rather than the declared name."""
        return ShardedProtected._meta.triggers[0].get_pgid(ShardedProtected)

    @property
    def public_table(self):
        return PublicProtected._meta.db_table

    @property
    def public_trigger(self):
        return PublicProtected._meta.triggers[0].get_pgid(PublicProtected)
