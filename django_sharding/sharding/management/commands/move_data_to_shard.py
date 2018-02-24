import filecmp
from io import StringIO
from tempfile import NamedTemporaryFile

from django.contrib.admin.utils import NestedObjects
from django.core.management import BaseCommand, CommandError
from django.db import IntegrityError
from django.db.models import sql
from django.db.models.loading import get_model

from sharding import State, ShardingMode
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
        parser.add_argument('--quiet', action='store', dest='quiet', default=False, help='Suppress output.')
        parser.add_argument('--no_input', action='store', dest='no_input', default=False, help='Skip confirmation.')

    def handle(self, *args, **options):
        """
        Move data, based on model name and id, from the source shard to the target shard.
        Both shards are put into maintenance during this.
        Delete the original data and release the transaction only after the migration is verified.
        """
        self.quiet = options.get('quiet')
        model_name = options.get('model_name')
        root_object_id = options.get('root_object_id')

        source_shard_alias = options.get('source_shard_alias')
        source_shard = self.get_shard(alias=source_shard_alias)
        target_shard = self.get_target_shard(options=options)

        root_object = self.get_object(model_name, root_object_id, source_shard)
        data = self.get_data(source_shard=source_shard, root_object=root_object)

        if not options.get('no_input'):
            confirm = input("Type 'yes' if you are sure if you want to move the following data from {} to {}:\n{}: "
                            .format(source_shard_alias, target_shard.alias, data))
            if confirm != 'yes':
                return

        self.pre_execution(options=options, source_shard=source_shard, target_shard=target_shard,
                           root_object=root_object, data=data)
        try:
            # Pushing the node names through a set means we end up with a list of unique names.
            nodes = list(set([source_shard.node_name, target_shard.node_name]))
            with transaction_for_nodes(nodes=nodes):
                self.move_data(data=data, source_shard=source_shard, target_shard=target_shard)
                if not self.confirm_data_integrity(data=data, source_shard=source_shard, target_shard=target_shard):
                    raise IntegrityError('Data was not successfully copied.')
                self.delete_data(data=data, source_shard=source_shard)
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
        with use_shard(source_shard):
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
    def get_target_shard(options):
        """
        Get the target shard based on options.
        This function is here so one can easily override it for their own needs.
        """
        return Command.get_shard(alias=options.get('target_shard_alias'))

    @staticmethod
    def get_data(source_shard, root_object):
        """
        Use a collector to gather all data belonging to the given object.
        Return only the data dict.
        We also filter on Sharded models only. Though this is, if the target data model is correct, not needed:
        The collector doesn't collect m2m field targets.
        """
        sharded_models = get_all_sharded_models()

        with use_shard(source_shard):
            collector = NestedObjects(using=source_shard.node_name)
            collector.collect([root_object])

        return {model: instances for model, instances in collector.data.items() if model in sharded_models}

    def move_data(self, data, source_shard, target_shard):
        """
        Copy all given data from the source to the target. Delete the data on source after a successful copy.
        """
        if not self.quiet:
            print('Moving data from {} to {}'.format(source_shard, target_shard))
        for model, instances in data.items():
            if not self.quiet:
                print('Exporting {}.{}'.format(model._meta.app_label, model._meta.model_name))
            # Export
            pk_list = [obj.pk for obj in instances]
            io = StringIO()
            with use_shard(source_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(
                    'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT'.format(t=model._meta.db_table),  # nosec
                    [pk_list])
                self.copy_expert(env.connection.cursor(), query, io)
                # TODO(SHARDING-21) update sequencer for this table
                # This wouldn't be necessary if we would omit the ids when copying.

            # Import
            io.seek(0)
            with use_shard(target_shard, active_only_schemas=False) as env:
                self.copy_from(env.connection.cursor(), io, model._meta.db_table)

    def confirm_data_integrity(self, data, source_shard, target_shard):
        """
        Let both the source_shard and the target_shard export the data to a file and compare them.
        """
        if not self.quiet:
            print('Confirming data integrity')

        # With TemporaryFile
        with NamedTemporaryFile() as source_file, NamedTemporaryFile() as target_file:
            for model, instances in data.items():  # nosec
                # Export
                pk_list = [obj.pk for obj in instances]
                query_string = 'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT' \
                    .format(t=model._meta.db_table)

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
        It uses the function `objects.delete()` would.
        """
        if not self.quiet:
            print('Deleting exported data')
        for model, instances in data.items():
            query = sql.DeleteQuery(model)
            pk_list = [obj.pk for obj in instances]
            with use_shard(source_shard, active_only_schemas=False):
                # Providing a 'using' to delete_batch is not needed for us, but the function expects it.
                query.delete_batch(pk_list, using=source_shard.node_name)

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

    @staticmethod
    def copy_from(cursor, *args, **kwargs):
        """
        Make this mockable by giving it a separate function.
        """
        cursor.copy_from(*args, **kwargs)
