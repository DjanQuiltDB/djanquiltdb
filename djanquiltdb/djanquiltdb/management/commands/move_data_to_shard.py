import csv
import filecmp
import functools
import subprocess  # nosec
from io import StringIO
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir

import progressbar
from django.apps import apps
from django.contrib.admin.utils import NestedObjects
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError
from django.db import IntegrityError, connections

from djanquiltdb import State, ShardingMode
from djanquiltdb.collector import SimpleCollector
from djanquiltdb.utils import use_shard, get_shard_class, get_all_sharded_models, get_mapping_class, \
    get_model_sharding_mode, transaction_for_nodes


def indent(text, indentation=1):
    return '{}{}'.format(' ' * indentation * 4, text)


def color(text, code):
    return '\033[{}m{}\033[0m'.format(code, text)


green = functools.partial(color, code='32')
magenta = functools.partial(color, code='35')
bold = functools.partial(color, code='1')


class Command(BaseCommand):
    """
    This command migrates all data belonging to a single root_object from one shard to another.
    A root_object is the top object of a data hierarchy.

    Both the source and target shard are put into maintenance mode during the migration.
    """
    help = 'Move all data belonging to a single root_object from one shard to another.'

    def add_arguments(self, parser):
        parser.add_argument('--source-shard-alias', '-n', action='store', dest='source_shard_alias',
                            help='Name of the shard where the root_object will be migrated from.', required=True)
        parser.add_argument('--target-shard-alias', '-t', action='store', dest='target_shard_alias',
                            help='Name of the shard which will receive the data.', required=True)
        parser.add_argument('--model-name', action='store', dest='model_name', required=True,
                            help='app_label.model_name of the root object.')
        parser.add_argument('--root-object-id', action='store', dest='root_object_id', help='ID of the root object.',
                            required=True)
        parser.add_argument('--reuse-simple-collector-for-delete',
                            action='store_true',
                            dest='reuse_simple_collector_for_delete',
                            help="Do not use Django's original delete collector to determine what needs to be "
                                 "deleted from the source_shard, but reuse the data collector for copy.",
                            default=False)
        parser.add_argument('-q', '--quiet', '--silent', action='store_true', dest='quiet', help='Suppress output.',
                            default=False)
        parser.add_argument('--noinput', '--no-input', action='store_true', dest='no_input', help='Skip confirmation.',
                            default=False)
        parser.add_argument('--nodelete', '--no-delete',
                            action='store_true',
                            dest='no_delete',
                            help='Skip deleting data from the old shard.',
                            default=False)
        parser.add_argument('--keep-validation-files', action='store_true', dest='keep_validation_files',
                            help='Keep the two artifacts the validation step creates in /tmp/ containing the postgres '
                                 'dump of all collected and written data.',
                            default=False)

    def handle(self, *args, **options):
        """
        Move data, based on model name and id, from the source shard to the target shard.
        Both shards are put into maintenance during this.
        Delete the original data (if --no-delete is not provided) and release the transaction only after the migration
        is verified.
        """
        self.quiet = options['quiet']
        self.no_input = options['no_input']
        self.reuse_data = options['reuse_simple_collector_for_delete']
        self.root_object_id = options['root_object_id']
        self.old_source_state = {}  # Used to keep track of the old source states

        self.model_name = options['model_name']
        source_shard_alias = options['source_shard_alias']

        if not self.no_input:
            confirm = input("This command will move data from one shard to another. This will start with putting the "
                            "shards (and if applicable, mapping objects) in maintenance and acquiring an exclusive "
                            "lock. Type 'yes' if you want to continue: ")
            if confirm != 'yes':
                return

        self.source_shard = self.get_shard(alias=source_shard_alias)
        self.target_shard = self.get_target_shard(options=options)

        if self.source_shard.node_name != self.target_shard.node_name:
            raise ValidationError(f'The source shard {self.source_shard} and target shard {self.target_shard} are on '
                                  'different database nodes. This command does not work across nodes.'
                                  'Move data to a shard on the same node as the source, then use the '
                                  'move_shard_to_node command to migrate the data to a different node.')

        objects = self.get_objects(self.source_shard)

        self.pre_execution(root_objects=objects)

        try:
            self.print('Gathering data:')

            collector = self.get_data_collector(objects=objects)
            data = collector.data

            # Get the pk_set, which is needed for moving and confirming the data integrity
            with use_shard(self.source_shard, active_only_schemas=False, lock=False):
                pk_set = {model: [instance.pk for instance in instances] for model, instances in data.items()}

            if not self.confirm(data):
                # We don't want to continue, let's make sure we release the locks and set the shards to active again
                self.post_execution(succeeded=False)
                return

            # Pushing the node names through a set means we end up with a list of unique names.
            nodes = list({self.source_shard.node_name, self.target_shard.node_name})
            with transaction_for_nodes(nodes=nodes):
                model_fields = self.copy_data(pk_set=pk_set)
                self.reset_sequencers(data=data)

                if not self.confirm_data_integrity(pk_set=pk_set, model_fields=model_fields, options=options):
                    raise IntegrityError('Data was not successfully copied.')

                if not options.get('no_delete'):
                    # Delete the data. Pick the current collector if we reuse the data and pick Django's nested
                    # collector if we want to use the original collector.
                    delete_collector = collector if self.reuse_data else \
                        self.get_data_collector(objects=objects, use_original_collector=True)
                    self.delete_data(collector=delete_collector)
                else:
                    self.print('Skipped deleting data from the source shard.')
        except Exception as error:
            self.post_execution(succeeded=False)
            raise error
        else:
            self.post_execution(succeeded=True)

        self.print(green('Done. Moved {} data points'.format(sum(map(len, data.values())))))

    def print(self, *args):
        if not self.quiet:
            print(*args)

    def confirm(self, data):
        if not self.no_input or not self.quiet:
            print()
            print('Data:')
            for model, instances in data.items():
                print(magenta(model))
                print(indent('{} datapoints'.format(len(instances))))

            if not self.no_input:
                confirm = input("Type 'yes' if you are sure if you want to move this data from {} to {}: "
                                .format(bold(self.source_shard), bold(self.target_shard)))
                if confirm != 'yes':
                    return False

        return True

    def get_objects(self, shard):
        """
        Get the objects based on the top level object's model name and object id. Note that this returns a list of
        objects.
        """
        with use_shard(shard, active_only_schemas=False):
            model = apps.get_model(self.model_name)
            if not get_model_sharding_mode(model) == ShardingMode.SHARDED:
                raise CommandError("'{}' is not a sharded model.".format(model))
            return [model.objects.get(id=self.root_object_id)]

    @staticmethod
    def get_shard(alias):
        """
        Get shard based on alias.
        """
        shard = get_shard_class().objects.filter(alias=alias).first()
        if not shard:
            raise CommandError("No shard could be found with alias '{}'".format(alias))
        return shard

    @staticmethod
    def get_target_shard(options):
        """
        Get the target shard based on the root_object and options.
        This function is here so one can easily override it for their own needs.
        """
        return Command.get_shard(alias=options.get('target_shard_alias'))

    def get_data_collector(self, objects, use_original_collector=False):
        sharded_models = get_all_sharded_models(include_auto_created=True)

        with use_shard(self.source_shard, active_only_schemas=False, lock=False) as env:
            if use_original_collector:
                collector = NestedObjects(using=env.options)
            else:
                collector = SimpleCollector(connection=env.connection, verbose=(not self.quiet))

            # Collect the data
            collector.collect(objects)

            # And make sure we only collect data from sharded models
            for model in list(collector.data.keys()):
                if model not in sharded_models:
                    self.print('There might be something wrong with your data structure, because the collector '
                               'collected mirrored models. Check your data points closely to see if no unexpected '
                               'model instances are collected.')
                    del collector.data[model]

            return collector

    def copy_data(self, pk_set):
        """
        Copy all given data from the source to the target.
        Return a dict with the models as key and their exported fields as value.
        """
        model_fields = {}
        if not self.quiet:
            bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[progressbar.RotatingMarker(),
                         ' Moving data from {} to {}; '.format(self.source_shard, self.target_shard),
                         progressbar.Timer()])
        for model, pk_set in pk_set.items():
            # Export
            io = StringIO()
            with use_shard(self.source_shard, active_only_schemas=False, lock=False) as env:
                cursor = env.connection.cursor()
                query = cursor.mogrify(
                    'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) '  # nosec
                    'TO STDOUT WITH CSV DELIMITER \';\' HEADER'.format(  # nosec
                        t=model._meta.db_table),
                    [list(pk_set)])
                self.copy_data_stream(cursor, query.decode() if isinstance(query, bytes) else query, io)

            # Read the csv headers from the output, and save them as a list of field names.
            # We will use this for importing and validation.
            io.seek(0)
            reader = csv.reader(io, delimiter=';')
            fields = next(reader)
            fields = ','.join('"{}"'.format(f) for f in fields)
            model_fields[model] = fields

            # Import
            io.seek(0)
            with use_shard(self.target_shard, active_only_schemas=False, lock=False) as env:
                cursor = env.connection.cursor()
                copy_sql = 'COPY "{t}" ({f}) FROM STDIN WITH CSV DELIMITER \';\' HEADER'.format(  # nosec
                    t=model._meta.db_table, f=fields)
                self.copy_data_stream(cursor, copy_sql, io)

            if not self.quiet:
                bar.update()

        if not self.quiet:
            bar.finish()

        return model_fields

    def reset_sequencers(self, data):
        """
        Reset the sequencers for all models on the target schema
        """
        with use_shard(self.target_shard, active_only_schemas=False, lock=False) as env:
            env.connection.reset_sequence(model_list=list(data.keys()))

    def _sort(self, source_file_name):
        """
        PostgreSQL's copy command will dump the data for the given table rows into a file for us. But it does not do so
        in a guaranteed order. So if we were to compare two data dumps, as we do in confirm_data_integrity, than
        we will find the resulting files to differ in order. Since we only need to know the same data is in both tables,
        we do not care for the export order. We use the external 'sort' command to order the data dumps alphabetically,
        so make them identical to each other if their contents are the same.
        """
        source_file_sorted_name = '{}-sorted'.format(source_file_name)

        try:
            subprocess.run(  # nosec
                ['sort', source_file_name, '-o', source_file_sorted_name],
                shell=False,
                check=True,
                timeout=60
            )
        except (RuntimeError, FileNotFoundError):
            raise CommandError("'sort' command is not available on your system")

        return source_file_sorted_name

    def validate_data_integrity(self, pk_set, model_fields, temp_dir):
        """
        Let both the source_shard and the target_shard export the data to a file and compare them.
        This happens in the following steps:
         - Create two temporary files
         - Per model, export the pk set to the files
         - Sort the two files to new files
         - Compare the two sorted files
        """
        if not self.quiet:
            bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[progressbar.RotatingMarker(),
                         ' Confirming data integrity; ',
                         progressbar.Timer()])

        with NamedTemporaryFile(dir=temp_dir, prefix='source_', delete=False) as source_file, \
                NamedTemporaryFile(dir=temp_dir, prefix='target_', delete=False) as target_file:
            for model, keys in pk_set.items():
                fields = model_fields.get(model)
                # Export
                query_string = 'COPY (SELECT {f} FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT'.format(  # nosec
                    t=model._meta.db_table, f=fields)

                # We let the copy functions just append to the output file
                with use_shard(self.source_shard, active_only_schemas=False, lock=False) as env:
                    cursor = env.connection.cursor()
                    query = cursor.mogrify(query_string, [keys])
                    self.copy_data_stream(cursor, query.decode() if isinstance(query, bytes) else query, source_file)

                with use_shard(self.target_shard, active_only_schemas=False, lock=False) as env:
                    cursor = env.connection.cursor()
                    query = cursor.mogrify(query_string, [keys])
                    self.copy_data_stream(cursor, query.decode() if isinstance(query, bytes) else query, target_file)

                if not self.quiet:
                    bar.update()

        # Sort the files alphabetically
        source_file_sorted_name = self._sort(source_file.name)
        if not self.quiet:
            bar.update()

        target_file_sorted_name = self._sort(target_file.name)
        if not self.quiet:
            bar.update()

        # Compare
        return filecmp.cmp(source_file_sorted_name, target_file_sorted_name)

    def confirm_data_integrity(self, pk_set, model_fields, options):
        """
        Call validate_data_integrity with or without a temporaryDirectory wrapped around it.
        For details about how the validation process works, see validate_data_integrity.
        """
        if not options.get('keep_validation_files', False):
            # We create a temporary directory so all temporary files and 'sort' artifacts are all neatly removed
            # at the end. Even when something fails halfway.
            with TemporaryDirectory(prefix='move_data_to_shard_validation_') as temp_dir:
                return self.validate_data_integrity(pk_set=pk_set, model_fields=model_fields, temp_dir=temp_dir)
        else:
            self.print('Validation artifacts can be found in: {}'.format(gettempdir()))
            return self.validate_data_integrity(pk_set=pk_set, model_fields=model_fields, temp_dir=None)

    def delete_data(self, collector):
        with use_shard(self.source_shard, active_only_schemas=False, lock=False):
            collector.delete()

    def pre_execution(self, root_objects):
        """
        Called before we enter the transaction.

        Before we set something into maintenance, we acquire an exclusive advisory lock.
        We do this so we wait until all current usages of the shard are done (since they set a shared lock),
        and cause all new usages to wait for this lock the be released.

        If there is a mapping model, we can set that into maintenance.
        If not, we set the source shard into maintenance.
        """
        source_connection = connections[self.source_shard.node_name]
        mapping_model = get_mapping_class()

        if not self.quiet:
            bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength,
                                          widgets=[progressbar.RotatingMarker(),
                                                   ' Acquiring lock; ',
                                                   progressbar.Timer()])

        if mapping_model:
            for root_object in root_objects:
                root_object_id = root_object.id
                mapping_object = mapping_model.objects.for_target(root_object_id)

                # Get exclusive advisory lock on the mapping object.
                source_connection.acquire_advisory_lock(key='mapping_{}'.format(root_object_id), shared=False)

                self.old_source_state[root_object_id] = mapping_object.state
                mapping_object.state = State.MAINTENANCE
                mapping_object.save(update_fields=['state'])
        else:
            # Get exclusive advisory lock on the sharding object.
            source_connection.acquire_advisory_lock(key='shard_{}'.format(self.source_shard.id), shared=False)

            self.old_shard_state = self.source_shard.state
            self.source_shard.state = State.MAINTENANCE
            self.source_shard.save(update_fields=['state'])

        if not self.quiet:
            bar.finish()

    def post_execution(self, succeeded):
        """
        Called after the transaction is committed. Both after success or failure.
        Set both shards back to their original state.
        Update the mapping object's shard field, if available.
        """
        source_connection = connections[self.source_shard.node_name]
        mapping_model = get_mapping_class()

        if mapping_model:
            # No need to fetch the objects from the shards, because we can use the old_source_state dictionary here to
            # release the locks and put the mapping objects back to their old state.
            for root_object_id, state in self.old_source_state.items():
                mapping_object = mapping_model.objects.for_target(root_object_id)
                mapping_object.state = state

                if succeeded:
                    mapping_object.shard = self.target_shard

                mapping_object.save(update_fields=['state', 'shard'])

                # Release the exclusive advisory lock
                source_connection.release_advisory_lock(key='mapping_{}'.format(root_object_id), shared=False)
        else:
            self.source_shard.state = self.old_shard_state
            self.source_shard.save(update_fields=['state'])
            source_connection.release_advisory_lock(key='shard_{}'.format(self.source_shard.id), shared=False)

    @staticmethod
    def copy_data_stream(cursor, sql, file_obj):
        """
        Copy data using psycopg3's copy() API.
        For export (TO STDOUT): reads from copy and writes to file_obj
        For import (FROM STDIN): reads from file_obj and writes to copy
        """
        with cursor.copy(sql) as copy:
            # Check if it's TO STDOUT (export) or FROM STDIN (import)
            if 'TO STDOUT' in sql.upper():
                # Export: read from copy and write to file_obj
                # psycopg3 copy.read() returns bytes or memoryview
                while True:
                    data = copy.read()
                    if not data:
                        break
                    # Convert memoryview to bytes if needed
                    if isinstance(data, memoryview):
                        data = data.tobytes()
                    elif not isinstance(data, bytes):
                        data = bytes(data)
                    # Check if file_obj is in binary mode
                    if hasattr(file_obj, 'mode') and 'b' in file_obj.mode:
                        file_obj.write(data)
                    else:
                        file_obj.write(data.decode('utf-8'))
            else:
                # Import: read from file_obj and write to copy
                while True:
                    data = file_obj.read(8192)  # Read in 8KB blocks
                    if not data:
                        break
                    # Ensure data is bytes
                    if isinstance(data, str):
                        data = data.encode('utf-8')
                    copy.write(data)
