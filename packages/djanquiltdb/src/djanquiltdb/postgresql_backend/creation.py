import hashlib
import json
from collections import defaultdict
from contextlib import nullcontext
from io import StringIO

from django.apps import apps
from django.conf import settings
from django.core import serializers
from django.db import router, transaction
from django.db.backends.postgresql.creation import DatabaseCreation as BaseDatabaseCreation

from djanquiltdb.management.base import shard_table_exists
from djanquiltdb.management.executor import enable_shared_migration_states
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import create_template_schema, get_shard_class, get_template_name, use_shard


class DatabaseCreation(BaseDatabaseCreation):
    def create_test_db(self, verbosity=1, autoclobber=False, serialize=None, keepdb=False):
        """
        Build the test database. When the database is new and QUILT_DB['SHARED_TEST_MIGRATION_STATES'] is True, migrate
        shares the project states of each migration between schemas (see the migrations documentation).

        When serialize is not given, it is not passed on either, so Django's own default applies.
        """
        if keepdb or not settings.QUILT_DB.get('SHARED_TEST_MIGRATION_STATES', False):
            context = nullcontext()
        else:
            context = enable_shared_migration_states()

        kwargs = {} if serialize is None else {'serialize': serialize}
        with context:
            return super().create_test_db(verbosity=verbosity, autoclobber=autoclobber, keepdb=keepdb, **kwargs)

    def get_migration_state_hash(self):
        """
        Return a hash of the migrations recorded in all schemas of the connected database.
        """
        quote_name = self.connection.ops.quote_name
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'django_migrations' "
                'ORDER BY table_schema'
            )
            rows = []
            for (schema_name,) in cursor.fetchall():
                cursor.execute(
                    'SELECT app, name FROM {}.django_migrations ORDER BY app, name'.format(quote_name(schema_name))
                )
                rows += [[schema_name, app, name] for app, name in cursor.fetchall()]

        return hashlib.sha256(json.dumps(rows).encode()).hexdigest()

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        """
        Clone the test database for a parallel test run, and store its migration state as a comment on the clone.

        With keepdb, Django reuses an existing clone as is, even though the test database itself gets migrated on every
        run. So when the stored state of a clone differs from the test database, we drop the clone first and let Django
        clone the test database again.
        """
        state = self.get_migration_state_hash()
        clone_name = self.get_test_db_clone_settings(suffix)['NAME']

        with self._nodb_cursor() as cursor:
            cursor.execute(
                "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = %s", [clone_name]
            )
            row = cursor.fetchone()
            if keepdb and row and row[0] != state:
                if verbosity >= 1:
                    self.log(
                        'Destroying outdated test database for alias {}...'.format(
                            self._get_database_display_str(verbosity, clone_name)
                        )
                    )
                cursor.execute('DROP DATABASE {}'.format(self._quote_name(clone_name)))

        super()._clone_test_db(suffix, verbosity, keepdb=keepdb)

        with self._nodb_cursor() as cursor:
            cursor.execute("COMMENT ON DATABASE {} IS '{}'".format(self._quote_name(clone_name), state))

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
