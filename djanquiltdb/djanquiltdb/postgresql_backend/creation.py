import functools
import json
from collections import defaultdict

from django.conf import settings
from django.db.backends.postgresql.creation import DatabaseCreation as BaseDatabaseCreation

from djanquiltdb.management.base import shard_table_exists
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, get_shard_class, get_template_name, use_shard


class DatabaseCreation(BaseDatabaseCreation):
    def serialize_db_to_string(self):
        _use_shard = functools.partial(use_shard, node_name=self.connection.alias)

        data = defaultdict(list)

        # Public schema
        with _use_shard(schema_name=PUBLIC_SCHEMA_NAME):
            data[PUBLIC_SCHEMA_NAME] = json.loads(super().serialize_db_to_string())

        # Template schema
        template_name = get_template_name()
        if self.connection.get_ps_schema(template_name):
            with _use_shard(schema_name=template_name, include_public=False):
                data[template_name] = json.loads(super().serialize_db_to_string())

        # Shards
        if shard_table_exists():
            for shard in get_shard_class().objects.filter(node_name=self.connection.alias):
                with _use_shard(shard, active_only_schemas=False, include_public=False):
                    data[shard.schema_name] = json.loads(super().serialize_db_to_string())

        return json.dumps(data)

    def deserialize_db_from_string(self, data):
        for schema_name, data_ in json.loads(data).items():
            with use_shard(node_name=self.connection.alias, schema_name=schema_name, active_only_schemas=False):
                super().deserialize_db_from_string(json.dumps(data_))


class TemplateDatabaseCreation(DatabaseCreation):
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Extend this method to create a template schema as well during test database creation. Note that the
        create_test_db would be a better place to put this in, but we do want the template schema to be created before
        we serialize the database, to make sure everything in the template schema will be serialized as well. We can do
        that in create_test_db, but that requires us to copy over everything that's in there, instead of simply hooking
        into this method.

        Note that testing this change is a big challenge, due to the fact that test db creation is done at the start of
        the test run, and calling create_test_db again will interfere with the current test run. Django itself has also
        minimal test coverage for this, and mostly test error paths.
        """
        test_database_name = super()._create_test_db(verbosity, autoclobber, keepdb=keepdb)

        # This is actually done in the `create_test_db`, but we need it now to be sure that we create a template schema
        # on the correct database. We do this by closing the current connect and setting a new target for the
        # connection that will be established when create_template_schema tries to use it.
        self.connection.close()
        settings.DATABASES[self.connection.alias]['NAME'] = test_database_name
        self.connection.settings_dict['NAME'] = test_database_name

        # We report migrate messages at one level lower than that requested.
        # This ensures we don't get flooded with messages during testing
        # (unless you really ask to be flooded).
        create_template_schema(
            node_name=self.connection.alias,
            verbosity=max(verbosity - 1, 0),
            migrate=False,  # Will be done in the migrate command
        )

        return test_database_name
