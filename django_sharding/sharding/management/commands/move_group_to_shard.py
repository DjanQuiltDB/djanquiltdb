import filecmp
import os

from django.contrib.admin.utils import NestedObjects
from django.core.management import BaseCommand, CommandError
from django.db import transaction, IntegrityError
from django.db.models import sql
from django.db.models.loading import get_model

from sharding import State
from sharding.utils import use_shard, get_shard_class, get_all_sharded_models


class Command(BaseCommand):
    """
    This command migrates all data belonging to a single group from one shard to another.
    A group is the top object of a data hierarchy.

    Both the source and target shard are put into maintenance mode during the migration.
    """
    help = 'Move all models flagged as sharded from public to a newly made sharded schema.'

    def add_arguments(self, parser):
        parser.add_argument('--source_shard_alias', action='store', dest='source_shard_alias',
                            help='Name of the shard where the group will be migrated from.')
        parser.add_argument('--target_shard_alias', action='store', dest='target_shard_alias',
                            help='Name of the shard which will receive the data.')
        parser.add_argument('--group_id', action='store', dest='group_id', help='ID of the top level group.')
        parser.add_argument('--model_name', action='store', dest='model_name',
                            help='app_label.model_name of the top level model.')
        parser.add_argument('--no_input', action='store', dest='no_input', default=False, help='Skip confirmation.')
        parser.add_argument('--silent', action='store', dest='silent', default=False, help='Suppress output.')

    def handle(self, *args, **options):
        """
        Move data, based on model anme and id, from the source shard to the target shard.
        Both shards are put into maintenance during this.
        Delete the original data and release the transaction only after the migration is verified.
        """
        self.silent = options.get('silent')
        model_name = options.get('model_name')
        group_id = options.get('group_id')

        source_shard_alias = options.get('source_shard_alias')
        source_shard = self.get_shard(alias=source_shard_alias)
        target_shard = self.get_target_shard(options=options)

        data = self.get_data(source_shard=source_shard, group=self.get_object(model_name, group_id, source_shard))

        if not options.get('no_input'):
            confirm = input("Type 'yes' if you are sure if you want to move the following data from {} to {}:\n{}: "
                            .format(source_shard_alias, target_shard.alias, data))
            if confirm != 'yes':
                return

        # Actual work happens here.
        self.pre_execution(options=options, source_shard=source_shard, target_shard=target_shard, data=data)
        try:
            with transaction.atomic(using=target_shard.node_name):
                self.move_data(data=data, source_shard=source_shard, target_shard=target_shard)
                if not self.confirm_data_integrity(data=data, source_shard=source_shard, target_shard=target_shard):
                    raise IntegrityError("Data was not successfully copied.")
                self.delete_data(data=data, source_shard=source_shard)
        finally:
            self.post_execution(options=options, source_shard=source_shard, target_shard=target_shard, data=data)

        if not self.silent:
            data_points = 0
            for _, instances in data.items():
                data_points += len(instances)
            print('Done. Moved {} data points'.format(data_points))

    def get_object(self, model_name, group_id, source_shard):
        """
        Get top level object based on model name and id.
        """
        with use_shard(source_shard):
            model = get_model(model_name)
            return model.objects.get(id=group_id)

    def get_shard(self, alias):
        """
        Get shard based on alias.
        """
        shard = get_shard_class().objects.filter(alias=alias).first()
        if not shard:
            raise CommandError("No shard could be found with alias '{}'".format(alias))
        return shard

    def get_target_shard(self, options):
        """
        Get the target shard based on options.
        """
        return self.get_shard(alias=options.get('target_shard_alias'))

    def get_data(self, source_shard, group):
        """
        Use a collector to gather all data belonging to the given object.
        Return only the data dict.
        We also filter on Sharded models only. Though this is, if the target data model is correct, not needed:
            The collector doesn't collect m2m field targets.
        """
        sharded_models = get_all_sharded_models()

        with use_shard(source_shard):
            collector = NestedObjects(using=source_shard.node_name)  # database name
            collector.collect([group])  # list of objects. single one won't do

        data = {model: instances for model, instances in collector.data.items() if model in sharded_models}
        return data

    def move_data(self, data, source_shard, target_shard):
        """
        Copy all given data from the source to the target. Delete the data on source after a successful copy.
        """
        if not self.silent:
            print('Moving data from {} to {}'.format(source_shard, target_shard))
        for model, instances in data.items():
            if not self.silent:
                print("Exporting {}.{}".format(model._meta.app_label, model._meta.model_name))
            # export
            pk_list = [obj.pk for obj in instances]
            io = open('/tmp/file_buffer.txt', 'w')
            with use_shard(source_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(
                    'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT'.format(t=model._meta.db_table),
                    [pk_list])
                self.copy_expert(env.connection.cursor(), query, io)
            io.close()

            io = open('/tmp/file_buffer.txt', 'r')
            # import
            with use_shard(target_shard, active_only_schemas=False) as env:
                self.copy_from(env.connection.cursor(), io, model._meta.db_table)
            io.close()
        os.remove('/tmp/file_buffer.txt')

    def confirm_data_integrity(self, data, source_shard, target_shard):
        """
        Let both the source_shard and the target_shard export the data to a file and compare them.
        """
        if not self.silent:
            print('Confirming data integrity')
        source_io = open('/tmp/source_buffer.txt', 'w')
        target_io = open('/tmp/target_buffer.txt', 'w')
        for model, instances in data.items():
            # export
            pk_list = [obj.pk for obj in instances]
            query_string = 'COPY (SELECT * FROM "{t}" WHERE "id" = ANY(%s)) TO STDOUT'.format(t=model._meta.db_table)

            # We let the copy functions just append to the output file
            with use_shard(source_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(query_string, [pk_list])
                self.copy_expert(env.connection.cursor(), query, source_io)
            with use_shard(target_shard, active_only_schemas=False) as env:
                query = env.connection.cursor().mogrify(query_string, [pk_list])
                self.copy_expert(env.connection.cursor(), query, target_io)

        source_io.close()
        target_io.close()

        result = filecmp.cmp('/tmp/source_buffer.txt', '/tmp/target_buffer.txt')

        os.remove('/tmp/source_buffer.txt')
        os.remove('/tmp/target_buffer.txt')

        return result

    def delete_data(self, data, source_shard):
        """
        Delete all the data given.
        It uses the function `objects.delete()` would.
        """
        if not self.silent:
            print('Deleting exported data')
        for model, instances in data.items():
            query = sql.DeleteQuery(model)
            pk_list = [obj.pk for obj in instances]
            with use_shard(source_shard, active_only_schemas=False):
                # Providing a 'using' to delete_batch is not needed for us, but the function expects it.
                query.delete_batch(pk_list, using=source_shard.node_name)

    def pre_execution(self, options, source_shard, target_shard, data):
        """
        Called before we enter the transaction.
        Set both shards in maintenance.
        """
        self.old_source_state = source_shard.state
        source_shard.state = State.MAINTENANCE
        source_shard.save(update_fields=['state'])

        self.old_target_state = target_shard.state
        target_shard.state = State.MAINTENANCE
        target_shard.save(update_fields=['state'])

    def post_execution(self, options, source_shard, target_shard, data):
        """
        Called after the transaction is committed. Both after success or failure.
        Set both shards in maintenance.
        """
        source_shard.state = self.old_source_state
        source_shard.save(update_fields=['state'])

        target_shard.state = self.old_target_state
        target_shard.save(update_fields=['state'])

    def copy_expert(self, cursor, *args, **kwargs):
        """
        Make this mockabe by giving it a separate function.
        """
        cursor.copy_expert(*args, **kwargs)

    def copy_from(self, cursor, *args, **kwargs):
        """
        Make this mockabe by giving it a separate function.
        """
        cursor.copy_from(*args, **kwargs)
