import logging

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.operations.base import Operation

from djanquiltdb import ShardingMode
from djanquiltdb.utils import get_sharding_mode, table_exists

logger = logging.getLogger(__name__)


class CreateMissingMirroredTable(Operation):
    """
    A migration operation that creates the table of a mirrored model in the public schema of each node that does not
    have it, and leaves the table as it is on the nodes that have it. It is for a model that had no sharding mode, whose
    table the default node has, so it raises ValueError for a model that is not mirrored now, and for a node that does
    not have the table while the default node does not have it either.

    The table is created as ``CreateModel`` creates it, with the columns, indexes and constraints of the model as the
    migrations before this one leave it. A new table has no rows. Reversing the operation does nothing, so that it never
    drops a table that has rows. See :ref:`making_an_existing_model_mirrored` for when a project needs it.

    A model that the app registry does not have, because a later migration renames or deletes it, is skipped with a
    warning, as the router skips an operation on such a model.
    """

    reversible = True
    reduces_to_sql = False

    def __init__(self, model_name):
        self.model_name = model_name

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        connection = schema_editor.connection

        # The migration state has no sharding modes, so the mode is the mode that the project declares for the model.
        try:
            sharding_mode = get_sharding_mode(app_label, model._meta.model_name)
        except LookupError:
            logger.warning(
                'CreateMissingMirroredTable skips %s, which the app registry does not have. Are you sure this model '
                'still exists?',
                model._meta.label,
            )
            return

        if sharding_mode is not ShardingMode.MIRRORED:
            raise ValueError(
                '{} is not mirrored. CreateMissingMirroredTable only creates the table of a model that is decorated '
                'with @mirrored_model().'.format(model._meta.label)
            )

        if not self.allow_migrate_model(connection.alias, model) or table_exists(connection, model._meta.db_table):
            return

        # A model without a sharding mode has its table on the default node only. When the default node has no table
        # either, the model was not such a model, and creating its table would hide that.
        if not table_exists(connections[DEFAULT_DB_ALIAS], model._meta.db_table):
            raise ValueError(
                'CreateMissingMirroredTable creates the table of a model that had no sharding mode, whose table is on '
                'the default node, but the default node has no table for {}.'.format(model._meta.label)
            )

        schema_editor.create_model(model)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def describe(self):
        return 'Create the table of {} on each node that does not have it'.format(self.model_name)

    @property
    def migration_name_fragment(self):
        return 'create_missing_table_{}'.format(self.model_name.lower())
