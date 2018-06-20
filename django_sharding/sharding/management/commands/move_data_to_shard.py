import collections
import csv
import filecmp
import functools
from io import StringIO
from tempfile import NamedTemporaryFile
import progressbar

from django.contrib.admin.utils import NestedObjects
from django.core.management import BaseCommand, CommandError
from django.db import IntegrityError, connections
from django.db.models import sql
from django.db.models.loading import get_model

from sharding import State, ShardingMode
from sharding.collector import SimpleCollector
from sharding.utils import use_shard, get_shard_class, get_all_sharded_models, get_mapping_class, \
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
        parser.add_argument('--source-shard-alias', action='store', dest='source_shard_alias',
                            help='Name of the shard where the root_object will be migrated from.')
        parser.add_argument('--target-shard-alias', action='store', dest='target_shard_alias',
                            help='Name of the shard which will receive the data.')
        parser.add_argument('--model-name', action='store', dest='model_name',
                            help='app_label.model_name of the root object.')
        parser.add_argument('--root-object-id', action='store', dest='root_object_id', help='ID of the root object.')
        parser.add_argument('--reuse-simple-collector-for-delete',
                            action='store',
                            dest='reuse_simple_collector_for_delete',
                            help="Do not use Django's original delete collector to determine what needs to be "
                                 "deleted from the source_shard, but reuse the data collector for copy.",
                            default=False)
        parser.add_argument('-q', '--quiet', '--silent', action='store_true', dest='quiet', help='Suppress output.',
                            default=False)
        parser.add_argument('--no-input', action='store_true', dest='no_input', help='Skip confirmation.',
                            default=False)

    def handle(self, *args, **options):
        """
        Move data, based on model name and id, from the source shard to the target shard.
        Both shards are put into maintenance during this.
        Delete the original data and release the transaction only after the migration is verified.
        """
        self.quiet = options.get('quiet')
        self.reuse_data = options.get('reuse_simple_collector_for_delete')
        model_name = options.get('model_name')
        root_object_id = options.get('root_object_id')

        source_shard_alias = options.get('source_shard_alias')
        source_shard = self.get_shard(alias=source_shard_alias)
        root_object = self.get_object(model_name, root_object_id, source_shard)
        target_shard = self.get_target_shard(root_object=root_object, options=options)

        if not options.get('no_input') or not self.quiet:
            confirm = input("This command will move data from one shard to another. This will start with putting the "
                            "shards (and if applicable, mapping objects) in maintenance and acquiring an exclusive "
                            "lock. Type 'yes' if you want to continue: ")
            if confirm != 'yes':
                return

        self.pre_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                           root_object=root_object)

        try:
            if not self.quiet:
                print('Gathering data:')

            data = self.get_data(source_shard=source_shard, root_objects=root_object)

            if not self.confirm(options, data, source_shard, target_shard):
                # We don't want to continue, let's make sure we release the locks and set the shards to active again
                self.post_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                                    root_object=root_object, succeeded=False)
                return

            # Pushing the node names through a set means we end up with a list of unique names.
            nodes = list({source_shard.node_name, target_shard.node_name})
            with transaction_for_nodes(nodes=nodes):
                model_fields = self.move_data(data=data, source_shard=source_shard, target_shard=target_shard)
                self.reset_sequencers(data=data, target_shard=target_shard)

                if not self.confirm_data_integrity(data=data, source_shard=source_shard, target_shard=target_shard,
                                                   model_fields=model_fields):
                    raise IntegrityError('Data was not successfully copied.')

                if self.reuse_data:
                    # Use the data we already collected to delete.
                    self.delete_data(data=data, source_shard=source_shard)
                else:
                    # Use Django's original collector to get the data to delete
                    self.delete_data(
                        data=self.get_data(source_shard=source_shard, root_objects=root_object,
                                           use_original_collector=True),
                        source_shard=source_shard)
        except Exception as error:
            self.post_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                                root_object=root_object, succeeded=False)
            raise error
        else:
            self.post_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                                root_object=root_object, succeeded=True)

        if not self.quiet:
            data_points = sum(map(len, data.values()))
            print(green('Done. Moved {} data points'.format(data_points)))

    def confirm(self, options, data, source_shard, target_shard):
        if not options.get('no_input') or not self.quiet:
            print()
            print('Data:')
            for model, instances in data.items():
                print(magenta(model))
                print(indent('{} datapoints'.format(len(instances))))

            if not options.get('no_input'):
                confirm = input("Type 'yes' if you are sure if you want to move this data from {} to {}: "
                                .format(bold(source_shard), bold(target_shard)))
                if confirm != 'yes':
                    return False

        return True

    @staticmethod
    def get_object(model_name, root_object_id, source_shard):
        """
        Get top level object based on model name and id.
        """
        with use_shard(source_shard, active_only_schemas=False):
            model = get_model(model_name)
            if not get_model_sharding_mode(model) == ShardingMode.SHARDED:
                raise CommandError("'{}' is not a sharded model.".format(model))
            return model.objects.get(id=root_object_id)

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
    def get_target_shard(root_object, options):
        """
        Get the target shard based on the root_object and options.
        This function is here so one can easily override it for their own needs.
        """
        return Command.get_shard(alias=options.get('target_shard_alias'))

    def get_data(self, source_shard, root_objects, use_original_collector=False):
        """
        Use the simple collector or Django's delete collector to gather all data belonging to the given object.
        Return only the data dict.
        We also filter on Sharded models only.
        """
        sharded_models = get_all_sharded_models()
        objects = root_objects if isinstance(root_objects, collections.Iterable) else [root_objects]

        with use_shard(source_shard, active_only_schemas=False) as env:
            if use_original_collector:
                collector = NestedObjects(using=source_shard.node_name)
                collector.collect(objects)

                # If possible, bring the models in an order suitable for databases that
                # don't support transactions or cannot defer constraint checks until the
                # end of a transaction.
                collector.sort()

                return {model: {i.pk for i in instances} for model, instances in collector.data.items()
                        if model in sharded_models}
            else:
                collector = SimpleCollector(connection=env.connection, verbose=(not self.quiet))
                collector.collect(objects)
                return {model: pk_set for model, pk_set in collector.data.items() if model in sharded_models}

    def move_data(self, data, source_shard, target_shard):
        """
        Copy all given data from the source to the target. Delete the data on source after a successful copy.
        Return a dict with the models as key and their exported fields as value.
        """
        model_fields = {}
        if not self.quiet:
            bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[progressbar.RotatingMarker(),
                         ' Moving data from {} to {}; '.format(source_shard, target_shard),
                         progressbar.Timer()])
        for model, pk_set in data.items():
            # Export
            io = StringIO()
            with use_shard(source_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(
                    'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) '  # nosec
                    'TO STDOUT WITH CSV DELIMITER \';\' HEADER'.format(  # nosec
                        t=model._meta.db_table),
                    [list(pk_set)])
                self.copy_expert(env.connection.cursor(), query, io)

            # Read the csv headers from the output, and save them as a list of field names.
            # We will use this for importing and validation.
            io.seek(0)
            reader = csv.reader(io, delimiter=';')
            fields = next(reader)
            fields = ','.join('"{}"'.format(f) for f in fields)
            model_fields[model] = fields

            # Import
            io.seek(0)
            with use_shard(target_shard, active_only_schemas=False) as env:
                self.copy_expert(env.connection.cursor(),
                                 'COPY "{t}" ({f}) FROM STDIN WITH CSV DELIMITER \';\' HEADER'  # nosec
                                 .format(t=model._meta.db_table, f=fields), io)

            if not self.quiet:
                bar.update()

        if not self.quiet:
            bar.finish()

        return model_fields

    def reset_sequencers(self, data, target_shard):
        """
        Reset the sequencers for all models on the target schema
        """
        models = [model for model, _ in data.items()]
        with use_shard(target_shard, active_only_schemas=False) as env:
            env.connection.reset_sequence(model_list=models)

    def confirm_data_integrity(self, data, source_shard, target_shard, model_fields):
        """
        Let both the source_shard and the target_shard export the data to a file and compare them.
        """
        if not self.quiet:
            bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[progressbar.RotatingMarker(),
                         ' Confirming data integrity; ',
                         progressbar.Timer()])

        with NamedTemporaryFile() as source_file, NamedTemporaryFile() as target_file:
            for model, pk_set in data.items():
                fields = model_fields.get(model)
                pk_list = list(pk_set)
                # Export
                query_string = 'COPY (SELECT {f} FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT'.format(  # nosec
                    t=model._meta.db_table, f=fields)

                # We let the copy functions just append to the output file
                with use_shard(source_shard, active_only_schemas=False) as env:
                    query = env.connection.cursor().mogrify(query_string, [pk_list])
                    self.copy_expert(env.connection.cursor(), query, source_file)

                with use_shard(target_shard, active_only_schemas=False) as env:
                    query = env.connection.cursor().mogrify(query_string, [pk_list])
                    self.copy_expert(env.connection.cursor(), query, target_file)

                if not self.quiet:
                    bar.update()

            source_file.seek(0)
            target_file.seek(0)

            # Tempfiles are removed when we leave their context manager.
            return filecmp.cmp(source_file.name, target_file.name)

    def delete_data(self, data, source_shard):
        """
        Delete all the data given.
        It calls delete_batch(pk_set) on each model in the data set.
        """
        if not self.quiet:
            bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[progressbar.RotatingMarker(),
                         ' Deleting exported data; ',
                         progressbar.Timer()])

        for model, pk_set in data.items():
            query = sql.DeleteQuery(model)
            with use_shard(source_shard, active_only_schemas=False):
                # Providing a 'using' to delete_batch is not needed for us, but the function expects it.
                query.delete_batch(list(pk_set), using=source_shard.node_name)

            if not self.quiet:
                bar.update()

    def pre_execution(self, options, source_shard, target_shard, root_object):
        """
        Called before we enter the transaction.

        Before we set something into maintenance, we acquire an exclusive advisory lock.
        We do this so we wait until all current usages of the shard are done (since they set a shared lock),
        and cause all new usages to wait for this lock the be released.

        If there is a mapping model, we can set that into maintenance.
        If not, we set the source shard into maintenance.
        """
        source_connection = connections[source_shard.node_name]
        mapping_model = get_mapping_class()

        if not self.quiet:
            bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength,
                                          widgets=[progressbar.RotatingMarker(),
                                                   ' Acquiring lock; ',
                                                   progressbar.Timer()])

        if mapping_model:
            mapping_object = mapping_model.objects.select_related('shard').for_target(root_object.id)

            # Get exclusive advisory lock on the mapping object.
            source_connection.acquire_advisory_lock(key='mapping_{}'.format(root_object.id), shared=False)

            self.old_source_state = mapping_object.state
            mapping_object.state = State.MAINTENANCE
            mapping_object.save(update_fields=['state'])
        else:
            # Get exclusive advisory lock on the sharding object.
            source_connection.acquire_advisory_lock(key='shard_{}'.format(source_shard.id), shared=False)

            self.old_source_state = source_shard.state
            source_shard.state = State.MAINTENANCE
            source_shard.save(update_fields=['state'])

        if not self.quiet:
            bar.finish()

    def post_execution(self, options, source_shard, target_shard, root_object, succeeded):
        """
        Called after the transaction is committed. Both after success or failure.
        Set both shards in maintenance.
        Update the mapping object's Shard field, if available.
        """
        source_connection = connections[source_shard.node_name]
        mapping_model = get_mapping_class()
        if mapping_model:
            mapping_object = mapping_model.objects.select_related('shard').for_target(root_object.id)
            mapping_object.state = self.old_source_state
            if succeeded:
                mapping_object.shard = target_shard
            mapping_object.save(update_fields=['state', 'shard'])

            # Release the exclusive advisory lock
            source_connection.release_advisory_lock(key='mapping_{}'.format(root_object.id), shared=False)
        else:
            source_shard.state = self.old_source_state
            source_shard.save(update_fields=['state'])
            source_connection.release_advisory_lock(key='shard_{}'.format(source_shard.id), shared=False)

    @staticmethod
    def copy_expert(cursor, *args, **kwargs):
        """
        Make this mockable by giving it a separate function.
        """
        cursor.copy_expert(*args, **kwargs)
