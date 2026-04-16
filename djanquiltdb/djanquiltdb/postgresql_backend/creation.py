import json
from collections import defaultdict
from io import StringIO

from django.apps import apps
from django.conf import settings
from django.core import serializers
from django.db import router, transaction
from django.db.backends.postgresql.creation import DatabaseCreation as BaseDatabaseCreation

from djanquiltdb.management.base import shard_table_exists
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, get_shard_class, get_template_name, use_shard


class DatabaseCreation(BaseDatabaseCreation):
    def _serialize_for_schema(self, schema_alias):
        """
        Serialize all models that belong to a specific schema.

        Uses the schema's full alias (e.g. 'default|org_1_schema') for both allow_migrate_model and queryset routing so
        the router correctly includes sharded models for shard schemas and queries read from the correct schema's
        tables.
        """
        from django.db.migrations.loader import MigrationLoader

        loader = MigrationLoader(self.connection)

        def get_objects():
            for app_config in apps.get_app_configs():
                if (
                    app_config.models_module is not None
                    and app_config.label in loader.migrated_apps
                    and app_config.name not in settings.TEST_NON_SERIALIZED_APPS
                ):
                    for model in app_config.get_models():
                        if model._meta.can_migrate(self.connection) and router.allow_migrate_model(schema_alias, model):
                            queryset = model._base_manager.using(schema_alias).order_by(model._meta.pk.name)
                            yield from queryset

        out = StringIO()
        serializers.serialize('json', get_objects(), indent=None, stream=out)
        return out.getvalue()

    def serialize_db_to_string(self):
        """
        Serialize all schemas into a single JSON string.

        Uses each schema's full alias (e.g. 'default|org_1_schema') for both allow_migrate_model checks and queryset
        routing, so sharded models are correctly included when serializing shard and template schemas.
        """
        node_name = self.connection.alias

        data = defaultdict(list)

        # Public schema: mirrored and public models
        data[PUBLIC_SCHEMA_NAME] = json.loads(self._serialize_for_schema(node_name))

        # Template schema: sharded models
        template_name = get_template_name()
        if self.connection.get_ps_schema(template_name):
            data[template_name] = json.loads(self._serialize_for_schema(f'{node_name}|{template_name}'))

        # Shards: sharded models
        if shard_table_exists():
            for shard in get_shard_class().objects.filter(node_name=node_name):
                data[shard.schema_name] = json.loads(self._serialize_for_schema(f'{node_name}|{shard.schema_name}'))

        return json.dumps(data)

    def deserialize_db_from_string(self, data):
        """
        Restore serialized DB state per-schema.

        Uses use_shard to set the active connection so the DynamicDbRouter routes saves to the correct schema. Wraps
        each schema's restore in a deferred-constraints transaction so model ordering (e.g. auth_permission before
        contenttypes_contenttype) doesn't cause FK violations.
        """
        for schema_name, schema_data in json.loads(data).items():
            with use_shard(node_name=self.connection.alias, schema_name=schema_name, active_only_schemas=False):
                with transaction.atomic(using=self.connection.alias):
                    with self.connection.cursor() as cursor:
                        cursor.execute('SET CONSTRAINTS ALL DEFERRED')
                    for obj in serializers.deserialize('json', json.dumps(schema_data), using=self.connection.alias):
                        obj.save()


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
