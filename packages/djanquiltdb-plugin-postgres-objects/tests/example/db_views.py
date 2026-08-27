# These declarations are fixtures, not schema: do not write them into a migration, however much `makemigrations`
# wants to. Every one of them reads SOURCE_TABLE, which is no model's table - a test creates it in setUp and drops it
# again afterwards - so a migration creating these views could never apply, and `migrate` would fail on the public
# schema, the template and every shard before a test database could be built. The cases that do need a migration use
# the `declared` app instead, whose fixture migrations a test points MIGRATION_MODULES at.

from djanquiltdb.decorators import mirrored_view, public_view, sharded_view
from postgres_objects import MaterializedView, View

APP_LABEL = 'example'
SOURCE_TABLE = 'view_source_table'


@public_view()
class PublicOnly(View):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)


@sharded_view()
class ShardOnly(View):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)


class Unannotated(View):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)


@public_view()
class PublicStored(MaterializedView):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)
    unique_index = ('id',)
    indexes = (('name',),)


@sharded_view()
class ShardStored(MaterializedView):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)
    unique_index = ('id',)
    indexes = (('name',),)


@mirrored_view()
class MirroredStored(MaterializedView):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)


@sharded_view()
class EmptyStored(MaterializedView):
    app_label = APP_LABEL
    sql = 'SELECT id, name FROM {}'.format(SOURCE_TABLE)
    with_data = False
