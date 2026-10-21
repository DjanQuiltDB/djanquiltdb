import contextlib
import os
import warnings
from collections import defaultdict

from django.conf import settings
from django.core import serializers
from django.core.management.base import CommandError
from django.core.management.commands.loaddata import Command as LoadDataCommand
from django.core.management.commands.loaddata import humanize
from django.core.serializers.python import Deserializer as PythonDeserializer
from django.db import connections

from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import (
    create_schema_on_node,
    create_template_schema,
    get_template_name,
    migrate_schema,
    use_shard,
)


class DeferredFieldsInSchema:
    """
    A deserialized object with deferred fields, which saves those fields in the schema that the object was loaded into.
    Django's loaddata saves the deferred fields of all objects with the ``using`` of --database.
    """

    def __init__(self, deserialized_object, shard_options):
        self.deserialized_object = deserialized_object
        self.shard_options = shard_options

    def save_deferred_fields(self, using=None):
        with use_shard(
            node_name=self.shard_options.node_name,
            schema_name=self.shard_options.schema_name,
            active_only_schemas=False,
        ) as env:
            self.deserialized_object.save_deferred_fields(using=env.options)


class Command(LoadDataCommand):
    def handle(self, *fixture_labels, **options):
        database = options.pop('database', None)

        if database is None:
            database = settings.QUILT_DB['PRIMARY_DB_ALIAS']

        # `database` can be a string ('other' or 'node|schema'), a tuple, a shard or a ShardOptions instance.
        # ShardOptions.from_alias() handles each of those and yields the node and schema the fixtures load into.
        shard_options = ShardOptions.from_alias(database)

        # The parent command expects `database` to be a string, and it doesn't work out-of-the-box with something
        # else. So we cast the database option to a string here.
        if not isinstance(database, str):
            database = '{}|{}'.format(shard_options.node_name, shard_options.schema_name)

        self._shard_options = shard_options
        # For each schema that objects are loaded into, the models of those objects, whose sequences are reset.
        self.models_loaded_into_each_schema = defaultdict(set)
        # The schemas that the node has, as far as this command checked or created them.
        self.existing_schema_names = {PUBLIC_SCHEMA_NAME}
        self.template_schema_is_migrated = False

        super().handle(*fixture_labels, database=database, **options)

    def load_label(self, fixture_label):
        """
        Load the fixture files of a label. This is the load_label of Django, except that each entry of a fixture is
        saved in the schema that its ``_schema`` key names, or in the schema of --database when it has no ``_schema``
        key. Each run of entries for the same schema is saved inside one use_shard of that schema.
        """
        show_progress = self.verbosity >= 3
        for fixture_file, fixture_dir, fixture_name in self.find_fixtures(fixture_label):
            _, ser_fmt, cmp_fmt = self.parse_name(os.path.basename(fixture_file))
            open_method, mode = self.compression_formats[cmp_fmt]
            fixture = open_method(fixture_file, mode)
            self.fixture_count += 1
            objects_in_fixture = 0
            loaded_objects_in_fixture = 0
            if self.verbosity >= 2:
                self.stdout.write(
                    "Installing %s fixture '%s' from %s." % (ser_fmt, fixture_name, humanize(fixture_dir))
                )
            try:
                objects = serializers.deserialize(
                    ser_fmt,
                    fixture,
                    using=self.using,
                    ignorenonexistent=self.ignore,
                    handle_forward_references=True,
                )

                with contextlib.ExitStack() as self.entered_use_shard:
                    schema_of_entered_use_shard = None

                    for shard_options, obj in self.deserialize_into_schemas(objects):
                        objects_in_fixture += 1

                        # create_missing_schema() closes this stack before it creates a schema that the node does not
                        # have, and that schema is never the entered one, because each entered schema exists already.
                        # So after a close there, the object that comes next is for another schema, and is saved only
                        # after this branch enters its schema.
                        if shard_options != schema_of_entered_use_shard:
                            self.entered_use_shard.close()
                            env = self.entered_use_shard.enter_context(
                                use_shard(
                                    node_name=shard_options.node_name,
                                    schema_name=shard_options.schema_name,
                                    active_only_schemas=False,
                                )
                            )
                            schema_of_entered_use_shard = shard_options

                        if self.save_obj_in_schema(obj, env.options):
                            loaded_objects_in_fixture += 1
                            if show_progress:
                                self.stdout.write(
                                    '\rProcessed %i object(s).' % loaded_objects_in_fixture,
                                    ending='',
                                )
            except Exception as e:
                if not isinstance(e, CommandError):
                    e.args = ("Problem installing fixture '%s': %s" % (fixture_file, e),)
                raise
            finally:
                fixture.close()

            if objects_in_fixture and show_progress:
                self.stdout.write()  # Add a newline after progress indicator.
            self.loaded_object_count += loaded_objects_in_fixture
            self.fixture_object_count += objects_in_fixture
            # Warn if the fixture we loaded contains 0 objects.
            if objects_in_fixture == 0:
                warnings.warn(
                    "No fixture data found for '%s'. (File format may be invalid.)" % fixture_name,
                    RuntimeWarning,
                )

    def deserialize_into_schemas(self, objects):
        """
        Yields each deserialized object of a fixture, with the options of the schema that the object is loaded into: the
        schema that the ``_schema`` key of its entry names, or the schema of --database. A schema that the node does not
        have yet is created before the object of its first entry is built.
        """
        if not isinstance(objects, PythonDeserializer):
            # A format whose deserializer is not built on the Python deserializer of Django, such as XML, has no
            # _schema key, so all its objects are loaded into the schema of --database.
            shard_options = self.shard_options_for_schema(self._shard_options.schema_name)
            self.create_missing_schema(shard_options)

            for obj in objects:
                yield shard_options, obj

            return

        # The JSON, JSON Lines and YAML deserializers of Django parse a fixture into the dicts in ``stream``, and build
        # the object of each dict with ``_handle_object()``, which looks up foreign keys and natural keys on ``using``.
        # Both names are private to Django. Setting ``using`` to the schema of each entry, before the object of that
        # entry is built, makes those lookups run in that schema.
        for entry in objects.stream:
            shard_options = self.shard_options_for_schema(entry.get('_schema', self._shard_options.schema_name))
            self.create_missing_schema(shard_options)
            objects.using = shard_options

            for obj in objects._handle_object(entry):
                yield shard_options, obj

    def shard_options_for_schema(self, schema_name):
        return ShardOptions(node_name=self._shard_options.node_name, schema_name=schema_name)

    def create_missing_schema(self, shard_options):
        """
        Creates the schema of ``shard_options`` when the node does not have it yet, as a clone of the template schema.
        Before the first schema is cloned, the template schema is created when it is missing, and migrated.
        """
        schema_name = shard_options.schema_name

        if schema_name in self.existing_schema_names:
            return

        node_name = shard_options.node_name

        if not connections[node_name].get_ps_schema(schema_name):
            # The entries before this one are saved, so the use_shard of their schema is left, and migrate runs outside
            # any shard. load_label() enters the new schema for the entry.
            self.entered_use_shard.close()

            if not self.template_schema_is_migrated:
                create_template_schema(node_name=node_name, migrate=False)
                migrate_schema(
                    node_name=node_name, schema_name=get_template_name(), interactive=False, verbosity=self.verbosity
                )
                self.template_schema_is_migrated = True

            # create_template_schema() has created the template schema itself.
            if schema_name != get_template_name():
                create_schema_on_node(schema_name=schema_name, node_name=node_name, migrate=True)

        self.existing_schema_names.add(schema_name)

    def save_obj_in_schema(self, obj, shard_options):
        """
        Saves an object with save_obj(), with ``self.using`` set to the options of the schema that the object is loaded
        into, so that the exclusions, the router and the save of Django's save_obj() apply to that schema.
        """
        using_of_database = self.using
        self.using = shard_options

        try:
            return self.save_obj(obj)
        finally:
            self.using = using_of_database

    def save_obj(self, obj):
        number_of_objs_with_deferred_fields = len(self.objs_with_deferred_fields)
        saved = super().save_obj(obj)

        if saved:
            self.models_loaded_into_each_schema[self.using].add(type(obj.object))

        # Django's save_obj() adds an object with deferred fields to objs_with_deferred_fields, unless --exclude
        # excludes the object. Django's loaddata() saves the deferred fields of those objects with the using of
        # --database, so the object that save_obj() added is replaced with a wrapper, which saves them in the schema of
        # the object.
        if len(self.objs_with_deferred_fields) > number_of_objs_with_deferred_fields:
            self.objs_with_deferred_fields[-1] = DeferredFieldsInSchema(obj, self.using)

        return saved

    def reset_sequences(self, connection, models):
        """
        Resets the sequences of the loaded models in each schema that objects were loaded into. Django's loaddata()
        passes the connection of --database and the models of all schemas.
        """
        for shard_options, schema_models in self.models_loaded_into_each_schema.items():
            connections[shard_options].reset_sequence(model_list=list(schema_models))
