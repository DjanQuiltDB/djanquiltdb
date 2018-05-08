import collections
import filecmp
from io import StringIO
from tempfile import NamedTemporaryFile

from django.contrib.admin.utils import NestedObjects
from django.core.management import BaseCommand, CommandError
from django.db import IntegrityError
from django.db.models import sql
from django.db.models.loading import get_model

from sharding import State, ShardingMode
from sharding.collector import SimpleCollector
from sharding.utils import use_shard, get_shard_class, get_all_sharded_models, get_mapping_class, \
    get_model_sharding_mode, transaction_for_nodes


class Command(BaseCommand):
    """
    This command migrates all data belonging to a single root_object from one shard to another.
    A root_object is the top object of a data hierarchy.

    Both the source and target shard are put into maintenance mode during the migration.
    """
    help = 'Move all data belonging to a single root_object from one shard to another.'

    def add_arguments(self, parser):
        parser.add_argument('--source_shard_alias', action='store', dest='source_shard_alias',
                            help='Name of the shard where the root_object will be migrated from.')
        parser.add_argument('--target_shard_alias', action='store', dest='target_shard_alias',
                            help='Name of the shard which will receive the data.')
        parser.add_argument('--model_name', action='store', dest='model_name',
                            help='app_label.model_name of the root object.')
        parser.add_argument('--root_object_id', action='store', dest='root_object_id', help='ID of the root object.')
        parser.add_argument('--reuse_simple_collector_for_delete',
                            action='store',
                            dest='reuse_simple_collector_for_delete',
                            help="Do not use Django's original delete collector to determine what needs to be "
                                 "deleted from the source_shard, but reuse the data collector for copy.",
                            default=False)
        parser.add_argument('--quiet', action='store', dest='quiet', default=False, help='Suppress output.')
        parser.add_argument('--no_input', action='store', dest='no_input', default=False, help='Skip confirmation.')

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

        data = self.get_data(source_shard=source_shard, root_objects=root_object)

        if not options.get('no_input'):
            print()
            print('Data:')
            for model, instances in data.items():
                print('\033[95m{}\033[0m'.format(model))
                print('    {} datapoints'.format(len(instances)))
            confirm = input("Type 'yes' if you are sure if you want to move this data "
                            "from \033[1m{}\033[0m to \033[1m{}\033[0m: "
                            .format(source_shard, target_shard))
            if confirm != 'yes':
                return

        self.pre_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                           root_object=root_object, data=data)
        try:
            # Pushing the node names through a set means we end up with a list of unique names.
            nodes = list(set([source_shard.node_name, target_shard.node_name]))
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

        finally:
            self.post_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                                root_object=root_object, data=data)

        if not self.quiet:
            data_points = sum(map(len, data.values()))
            print('Done. Moved {} data points'.format(data_points))

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
            print('Moving data from {} to {}'.format(source_shard, target_shard))
        for model, pk_set in data.items():
            if not self.quiet:
                print('Exporting {}.{}'.format(model._meta.app_label, model._meta.model_name))

            # Export
            io = StringIO()
            with use_shard(source_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(
                    'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT WITH CSV DELIMITER \';\' HEADER'  # nosec
                    .format(t=model._meta.db_table),
                    [list(pk_set)])
                self.copy_expert(env.connection.cursor(), query, io)

            # Read the csv headers from the output, and save them as a list of field names.
            # We will use this for importing and validation.
            io.seek(0)
            fields = str.replace(str.replace(io.readline(), ';', ','), '\n', '')
            model_fields[model] = fields

            # Import
            io.seek(0)
            with use_shard(target_shard, active_only_schemas=False) as env:
                self.copy_expert(env.connection.cursor(),
                                 'COPY "{t}" ({f}) FROM STDIN WITH CSV DELIMITER \';\' HEADER'  # nosec
                                 .format(t=model._meta.db_table, f=fields), io)

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
            print('Confirming data integrity')

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
            print('Deleting exported data')

        for model, pk_set in data.items():
            query = sql.DeleteQuery(model)
            with use_shard(source_shard, active_only_schemas=False):
                # Providing a 'using' to delete_batch is not needed for us, but the function expects it.
                query.delete_batch(list(pk_set), using=source_shard.node_name)

    def pre_execution(self, options, source_shard, target_shard, root_object, data):
        """
        Called before we enter the transaction.
        If there is a mapping model, we can set that into maintenance.
        If not, we set the source shard into maintenance.
        """
        mapping_model = get_mapping_class()
        if mapping_model:
            mapping_object = mapping_model.objects.select_related('shard').for_target(root_object.id)
            self.old_source_state = mapping_object.state
            mapping_object.state = State.MAINTENANCE
            mapping_object.save(update_fields=['state'])
        else:
            self.old_source_state = source_shard.state
            source_shard.state = State.MAINTENANCE
            source_shard.save(update_fields=['state'])

    def post_execution(self, options, source_shard, target_shard, root_object, data):
        """
        Called after the transaction is committed. Both after success or failure.
        Set both shards in maintenance.
        Update the mapping object's Shard field, if available.
        """
        mapping_model = get_mapping_class()
        if mapping_model:
            mapping_object = mapping_model.objects.select_related('shard').for_target(root_object.id)
            mapping_object.state = self.old_source_state
            mapping_object.shard = target_shard
            mapping_object.save(update_fields=['state', 'shard'])
        else:
            source_shard.state = self.old_source_state
            source_shard.save(update_fields=['state'])

    @staticmethod
    def copy_expert(cursor, *args, **kwargs):
        """
        Make this mockable by giving it a separate function.
        """
        cursor.copy_expert(*args, **kwargs)
