import json
import os
import tempfile
from collections import defaultdict

from django.conf import settings
from django.core.management.commands.loaddata import Command as LoadDataCommand
from django.core.serializers import get_deserializer
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


class Command(LoadDataCommand):
    def handle(self, *fixture_labels, **options):
        database = options.pop('database', None)

        # The command expects `database` to be a string, and it doesn't work out-of-the-box with something else. So we
        # cast the database options to a string here.
        if not isinstance(database, str):
            # `database` could be a tuple, a shard or ShardOptions instance. So we pass it to ShardOptions.from_alias(),
            # so we end up with a ShardOptions instance, from where we can get the node name and schema name.
            shard_options = ShardOptions.from_alias(database)
            database = '{}|{}'.format(shard_options.node_name, shard_options.schema_name)
        else:
            # Parse the database string to get node_name and schema_name
            shard_options = ShardOptions.from_alias(settings.QUILT_DB['PRIMARY_DB_ALIAS'])

        # Store shard_options for use in load_label
        self._shard_options = shard_options
        self._database_string = database
        # Track objects and fixtures loaded by our custom schema-aware loading
        self._custom_objects_loaded = 0
        self._custom_fixtures_loaded = 0

        # Call parent handle - it will call loaddata which calls our load_label
        super().handle(*fixture_labels, database=database, **options)

        # Report our count for schema-aware fixtures (parent doesn't see these)
        if self._custom_objects_loaded > 0 and self.verbosity >= 1:
            self.stdout.write(
                self.style.SUCCESS(
                    'Installed {} object(s) from {} schema-aware fixture(s)'.format(
                        self._custom_objects_loaded, self._custom_fixtures_loaded
                    )
                )
            )

    def load_label(self, fixture_label):
        """
        Load a fixture file, grouping entries by their _schema property and loading
        each group into the appropriate PostgreSQL schema.
        """
        # Find fixture files
        fixture_files = self.find_fixtures(fixture_label)
        if not fixture_files:
            # If no fixtures found, call parent to handle error
            return super().load_label(fixture_label)

        # Process each fixture file
        # find_fixtures returns tuples of (fixture_path, format, ...) with variable length
        for fixture_tuple in fixture_files:
            if isinstance(fixture_tuple, tuple):
                # Extract fixture_path (first element) and format (second element)
                # Handle tuples of different lengths (2 or 3 elements)
                fixture_file = fixture_tuple[0]
                # Get format from tuple if available, but validate it's a format string, not a path
                potential_format = fixture_tuple[1] if len(fixture_tuple) > 1 else None
                # Validate format is a known format string (not a path)
                valid_formats = {'json', 'xml', 'yaml', 'yml'}
                if potential_format and potential_format.lower() in valid_formats:
                    fixture_format = potential_format.lower()
                    # Normalize 'yml' to 'yaml' for Django's deserializer
                    if fixture_format == 'yml':
                        fixture_format = 'yaml'
                else:
                    fixture_format = None
            else:
                # Fallback for older Django versions that might return just paths
                fixture_file = fixture_tuple
                fixture_format = None
            self._load_fixture_with_schema_support(fixture_file, fixture_format)

    def _load_fixture_with_schema_support(self, fixture_file, fixture_format=None):
        """
        Load a fixture file, grouping entries by _schema property.
        Supports both JSON and YAML formats.
        """
        # Determine the format from the file extension if not provided
        if fixture_format is None:
            fixture_format = self._get_fixture_format(fixture_file)

        # Normalize 'yml' to 'yaml'
        if fixture_format == 'yml':
            fixture_format = 'yaml'

        # Only support JSON and YAML formats for _schema property
        # For other formats, load normally without schema support
        if fixture_format not in ('json', 'yaml'):
            self._load_fixture_normal(fixture_file, fixture_format)
            return

        # Read and parse the fixture file
        # Import yaml lazily if needed
        yaml = None
        if fixture_format == 'yaml':
            try:
                import yaml
            except ImportError:
                raise ImportError(
                    'PyYAML is required to load YAML fixtures. '
                    'Install it with: pip install PyYAML or pip install .[test]'
                )

        with open(fixture_file, 'r') as f:
            try:
                if fixture_format == 'yaml':
                    fixture_data = yaml.safe_load(f)
                else:  # json
                    fixture_data = json.load(f)
            except json.JSONDecodeError:
                # If JSON parsing fails, load normally
                self._load_fixture_normal(fixture_file, fixture_format)
                return
            except Exception as e:
                # Check if it's a YAML error (if yaml module is available)
                if yaml is not None and isinstance(e, yaml.YAMLError):
                    # If YAML parsing fails, load normally
                    self._load_fixture_normal(fixture_file, fixture_format)
                    return
                # Re-raise other exceptions
                raise

        # Handle None or empty data
        if fixture_data is None:
            return

        # Ensure fixture_data is a list
        if not isinstance(fixture_data, list):
            fixture_data = [fixture_data]

        # Track that we're processing a schema-aware fixture
        self._custom_fixtures_loaded += 1

        # Group entries by _schema property
        entries_by_schema = defaultdict(list)
        default_schema = self._shard_options.schema_name
        template_name = get_template_name()
        node_name = self._shard_options.node_name

        for entry in fixture_data:
            # Make a copy to avoid modifying the original
            entry_copy = entry.copy()
            # Extract _schema if present, otherwise use default schema
            schema_name = entry_copy.pop('_schema', default_schema)
            entries_by_schema[schema_name].append(entry_copy)

        # Ensure template schema exists and has migrations applied
        create_template_schema(node_name=node_name, interactive=False, verbosity=self.verbosity)
        # Ensure template schema has all migrations applied (in case it existed but migrations weren't up to date)
        connection = connections[node_name]
        if connection.get_ps_schema(template_name):
            # Migrate template schema to ensure it has all tables
            migrate_schema(node_name=node_name, schema_name=template_name, interactive=False, verbosity=self.verbosity)

        # Ensure all schemas exist and are cloned from template
        for schema_name in entries_by_schema.keys():
            # Skip public schema and template schema - they don't need to be cloned
            if schema_name in (PUBLIC_SCHEMA_NAME, template_name):
                continue

            # Check if schema exists
            if not connection.get_ps_schema(schema_name):
                # Create schema and clone from template
                create_schema_on_node(schema_name=schema_name, node_name=node_name, migrate=True)

        # Load each schema group and track counts
        total_objects_loaded = 0
        # Track models loaded per schema for sequence resetting
        models_by_schema = defaultdict(set)

        for schema_name, entries in entries_by_schema.items():
            # Determine file extension and serialization method based on original format
            if fixture_format == 'yaml':
                # yaml should already be imported at this point, but import lazily if needed
                if yaml is None:
                    try:
                        import yaml
                    except ImportError:
                        raise ImportError(
                            'PyYAML is required to load YAML fixtures. '
                            'Install it with: pip install PyYAML or pip install .[test]'
                        )
                file_suffix = '.yaml'
                # Create a temporary fixture file with only entries for this schema
                with tempfile.NamedTemporaryFile(mode='w', suffix=file_suffix, delete=False) as temp_file:
                    yaml.dump(entries, temp_file, default_flow_style=False, allow_unicode=True)
                    temp_file_path = temp_file.name
            else:  # json
                file_suffix = '.json'
                # Create a temporary fixture file with only entries for this schema
                with tempfile.NamedTemporaryFile(mode='w', suffix=file_suffix, delete=False) as temp_file:
                    json.dump(entries, temp_file, indent=2)
                    temp_file_path = temp_file.name

            deserializer = get_deserializer(fixture_format)

            try:
                with open(temp_file_path, 'r') as f:
                    if schema_name != PUBLIC_SCHEMA_NAME:
                        with use_shard(
                            node_name=self._shard_options.node_name, schema_name=schema_name, active_only_schemas=False
                        ) as env:
                            # Use Django's deserializer to load the fixture
                            # Get the connection alias from the use_shard context
                            try:
                                objects = deserializer(f, ignorenonexistent=self.ignore)
                                # Deserialize and save objects
                                # Convert to list to count, but iterate to save (preserves any counting in deserializer)
                                objects_list = []
                                for obj in objects:
                                    obj.save()
                                    objects_list.append(obj)
                                    # Track the model class for sequence resetting
                                    models_by_schema[schema_name].add(obj.object.__class__)
                                total_objects_loaded += len(objects_list)

                                # Reset sequences for models loaded in this schema
                                if models_by_schema[schema_name]:
                                    env.connection.reset_sequence(model_list=list(models_by_schema[schema_name]))
                            except Exception as e:
                                if self.verbosity >= 1:
                                    self.stdout.write(
                                        self.style.ERROR(
                                            "Problem installing fixture '%s' (schema '%s'): %s"
                                            % (temp_file_path, schema_name, e)
                                        )
                                    )
                                raise
                    else:
                        try:
                            objects = deserializer(f, ignorenonexistent=self.ignore)
                            # Deserialize and save objects
                            # Convert to list to count, but iterate to save (preserves any counting in deserializer)
                            objects_list = []
                            for obj in objects:
                                obj.save()
                                objects_list.append(obj)
                                # Track the model class for sequence resetting
                                models_by_schema[schema_name].add(obj.object.__class__)
                            total_objects_loaded += len(objects_list)

                            # Reset sequences for models loaded in public schema
                            if models_by_schema[schema_name]:
                                connection = connections[self._shard_options.node_name]
                                connection.reset_sequence(model_list=list(models_by_schema[schema_name]))
                        except Exception as e:
                            if self.verbosity >= 1:
                                self.stdout.write(
                                    self.style.ERROR(
                                        "Problem installing fixture '%s' (schema '%s'): %s"
                                        % (temp_file_path, schema_name, e)
                                    )
                                )
                            raise
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except OSError:
                    pass

        # Track objects loaded for reporting
        self._custom_objects_loaded += total_objects_loaded

    def _load_fixture_normal(self, fixture_file, fixture_format):
        """
        Load a fixture file normally without schema support.
        """
        deserializer = get_deserializer(fixture_format)
        with open(fixture_file, 'r') as f:
            try:
                objects = deserializer(f, using=self._database_string, ignorenonexistent=self.ignore)
                for obj in objects:
                    obj.save(using=self._database_string)
            except Exception as e:
                if self.verbosity >= 1:
                    self.stdout.write(self.style.ERROR("Problem installing fixture '%s': %s" % (fixture_file, e)))
                raise

    def _get_fixture_format(self, fixture_file):
        """Determine fixture format from file extension."""
        if fixture_file.endswith('.json'):
            return 'json'
        elif fixture_file.endswith('.xml'):
            return 'xml'
        elif fixture_file.endswith('.yaml') or fixture_file.endswith('.yml'):
            return 'yaml'  # Normalize both .yaml and .yml to 'yaml'
        else:
            return 'json'  # Default to json
