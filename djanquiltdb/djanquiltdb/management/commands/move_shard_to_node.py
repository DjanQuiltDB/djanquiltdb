import copy
import functools
from collections import defaultdict
from io import StringIO

import progressbar
from django.core.management import BaseCommand, CommandError
from django.db import connections

from djanquiltdb import State, ShardingMode
from djanquiltdb.options import ShardOptions
from djanquiltdb.utils import get_shard_class, get_all_databases, get_mapping_class, get_all_sharded_models, \
    get_all_public_models, create_schema_on_node, transaction_for_nodes, get_model_sharding_mode


def indent(text, indentation=1):
    return '{}{}'.format(' ' * indentation * 4, text)


def color(text, code):
    return '\033[{}m{}\033[0m'.format(code, text)


green = functools.partial(color, code='32')
gray = functools.partial(color, code='8')


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--source-shard-alias', '-n', action='store', dest='source_shard_alias',
                            help='Name of the shard which will move.', required=True)
        parser.add_argument('--target-node-alias', '-t', action='store', dest='target_node_alias',
                            help='Name of the node which will receive the shard.', required=True)
        parser.add_argument('--batch-size', '-b', action='store', dest='batch_size', type=int,
                            help='Size of the fetch batches during retargeting. 10k is the default', required=False,
                            default=10000)
        parser.add_argument('-q', '--quiet', '--silent', action='store_true', dest='quiet', help='Suppress output.',
                            default=False)
        parser.add_argument('--noinput', '--no-input', action='store_true', dest='no_input', help='Skip confirmation.',
                            default=False)

    def handle(self, *args, **options):
        self.quiet = options['quiet']
        self.no_input = options['no_input']
        self.batch_size = options['batch_size']

        source_shard_alias = options['source_shard_alias']
        self.source_shard = self.get_source_shard(alias=source_shard_alias)
        self.target_node = self.get_target_node(options=options)

        self.old_source_states = {}  # Used to keep track of the old source states

        if (not isinstance(self.batch_size, int)) or self.batch_size < 1:
            raise CommandError(f"Batch size must be an int of 1 or higher, not '{self.batch_size}'")

        if not self.no_input:
            confirm = input("This command will move a shard from one node to another. This will start with putting the "
                            "shard (and if applicable, all mapping objects on it) in maintenance and acquiring an "
                            "exclusive lock."
                            "Type 'yes' if you want to continue: ")
            if confirm != 'yes':
                return

        self.pre_execution()

        try:
            self.print('Moving shard...')
            self.move_shard()
        except Exception as error:
            self.post_execution(succeeded=False)
            raise error
        else:
            self.post_execution(succeeded=True)
        self.print(green(f'Done. Shard {self.source_shard} moved to node {self.target_node}'))

    def print(self, *args):
        if not self.quiet:
            print(*args)

    def bar_update(self, bar):
        if not self.quiet:
            bar.update()

    def bar_finish(self, bar):
        if not self.quiet:
            bar.finish()

    def get_source_shard(self, alias):
        """
        Get shard based on alias.
        """
        shard = get_shard_class().objects.filter(alias=alias).first()
        if not shard:
            raise CommandError("No shard could be found with alias '{}'".format(alias))
        return shard

    def get_target_node(self, options):
        node = options['target_node_alias']
        if node not in get_all_databases():
            raise CommandError("Could not find node '{}' in known set of databases".format(node))
        if connections[node].get_ps_schema(self.source_shard.schema_name):
            raise CommandError("Shard '{}' already exists on target node '{}'".format(self.source_shard, node))

        return node

    def pre_execution(self):
        """
        Called before we enter the transaction.

        Before we set something into maintenance, we acquire an exclusive advisory locks.
        We do this so we wait until all current usages of the shard are done (since they set a shared lock),
        and cause all new usages to wait for this lock the be released.

        We also set the shard in maintenance
        """
        source_connection = connections[self.source_shard.node_name]
        mapping_model = get_mapping_class()

        if not self.quiet:
            bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength,
                                          widgets=[progressbar.RotatingMarker(),
                                                   ' Acquiring locks; ',
                                                   progressbar.Timer()])

        if mapping_model:
            for mapping_object in mapping_model.objects.for_shard(self.source_shard).iterator():
                # Get exclusive advisory lock on the mapping object.
                source_connection.acquire_advisory_lock(key='mapping_{}'.format(mapping_object.id), shared=False)

                self.old_source_states[mapping_object.id] = mapping_object.state
                mapping_object.state = State.MAINTENANCE
                mapping_object.save(update_fields=['state'])

        # Get exclusive advisory lock on the sharding object.
        source_connection.acquire_advisory_lock(key='shard_{}'.format(self.source_shard.id), shared=False)

        self.old_shard_state = self.source_shard.state
        self.source_shard.state = State.MAINTENANCE
        self.source_shard.save(update_fields=['state'])

        if not self.quiet:
            bar.finish()

    def move_shard(self):
        """
        Steps to move a shard are as following (inside a transaction):
        1) Make a schema on the target node
        2) Copy the data from the source schema to the target schema
        3) Retarget the relations between the target schema and the public schema if needed
        4) Reset all sequences to match the new data
        5) Upon transaction close, all fkeys constrains will be checked by postgres
        """
        nodes = list({self.source_shard.node_name, self.target_node})
        with transaction_for_nodes(nodes=nodes):
            create_schema_on_node(schema_name=self.source_shard.schema_name,
                                  node_name=self.target_node,
                                  migrate=True)

            self.target_shard_options = ShardOptions(node_name=self.target_node,
                                                     schema_name=self.source_shard.schema_name)

            self.copy_data()
            self.retarget_relations()
            self.reset_sequences()

    def copy_data(self):
        """
        Copy all given data from the source to the target.
        Return a dict with the models as key and their exported fields as value.
        """
        bar = progressbar.ProgressBar(
            max_value=progressbar.UnknownLength,
            widgets=[progressbar.RotatingMarker(),
                     ' Moving data from {} to {}; '.format(self.source_shard, self.target_node),
                     progressbar.Timer()])

        with self.source_shard.use(include_public=False, active_only_schemas=False, lock=False) as env:
            tables = env.connection.get_all_table_headers()
            tables.remove('django_migrations')  # No need to copy this one. It's already done by clone_schema

        for table in tables:
            # Export
            io = StringIO()
            with self.source_shard.use(include_public=False, active_only_schemas=False, lock=False) as env:
                query = env.connection.cursor().mogrify(
                    'COPY (SELECT * FROM "{t}") '  # nosec
                    'TO STDOUT WITH (FORMAT CSV, DELIMITER \';\', HEADER, FORCE_QUOTE *)'.format(  # nosec
                        t=table))
                self.copy_expert(env.connection.cursor(), query, io)

            # Import
            io.seek(0)
            headers = io.readline().strip().split(';')
            headers = ', '.join([f'"{h}"' for h in headers])
            io.seek(0)
            with self.target_shard_options.use() as env:
                self.copy_expert(env.connection.cursor(),
                                 'COPY "{t}" ({headers}) FROM STDIN WITH (FORMAT CSV, DELIMITER \';\', HEADER)'  # nosec
                                 .format(t=table, headers=headers), io)

            self.bar_update(bar)

        self.bar_finish(bar)

    @staticmethod
    def get_related_model(field):
        # Django < 2.0
        remote_field = 'rel' if hasattr(field, 'rel') else 'remote_field'
        related_model = getattr(field, remote_field)
        if not related_model.concrete:  # auto_created
            # The model is a mapping table for a many-to-one/many-to-many relation.
            # We want the target model in this case
            return related_model.model

    def get_mapped_value(self, model, nat_keys_value):
        """
        Give a set of natural keys.
        Get mapped value (so id on node A -> natural keys, natural keys -> id on node B).
        If the target object does not exist on the target node, copy it.
        Regardless if the target object is fetches or copied, put it through the same retargeting process as the
        source object.
        """

        # If the natural key fields consist of a relation field. Fetch and translate that object first (recursive).
        org_nat_keys_value = copy.deepcopy(nat_keys_value)
        for index, field_name in enumerate(model._meta.unique_together[0]):
            field = model._meta._forward_fields_map[field_name]
            if field.is_relation:
                related_nat_keys_value = self.source_data[field.related_model].get(nat_keys_value[index])
                new_nat_keys_value = list(nat_keys_value)
                new_nat_keys_value[index] = self.get_mapped_value(field.related_model, related_nat_keys_value)
                nat_keys_value = tuple(new_nat_keys_value)

        mapped_value = self.target_data[model].get(nat_keys_value)

        # If the target data does not contain the natural keys, then the object does not exist on the target node.
        # If we are allowed to copy it, we do so here (and call for it's retargeting) and add it to the target_data
        # dict.
        if not mapped_value:
            if getattr(model, '__allow_copy', True):
                with self.source_shard.use(active_only_schemas=False, lock=False):
                    source_object = model.objects.get_by_natural_key(*org_nat_keys_value)
                    source_object.id = None
                    self.print(indent(gray(f'Creating <{source_object._meta.object_name}>{source_object}'), 2))
                    source_object.save(using=self.target_shard_options, force_insert=True)
                    self.print(indent(gray(f'Saved <{source_object._meta.object_name}>{source_object} to id '
                                           f'{source_object.id}'), 3))

                    self._check_relations(model, source_object)  # Recurse on the newly copied object.
                    mapped_value = source_object.id
                    self.target_data[model][nat_keys_value] = mapped_value
            else:
                with self.source_shard.use(active_only_schemas=False, lock=False):
                    source_object = model.objects.get_by_natural_key(*nat_keys_value)
                raise ValueError('Data "{}.{}: {} - {}" not found for on target shard "{}", and the model does not '
                                 'allow the data to be copied.'
                                 .format(model._meta.app_label, model.__name__, nat_keys_value, source_object,
                                         self.target_shard_options.node_name))

        return mapped_value

    def _check_relations(self, model, object):
        """
        For the given object, loop over each relation field and fetch the related objects that matches on the target
        node. Save the object with the new relations.
        """
        self.print(indent(gray(f'Retargeting <{object._meta.object_name}>{object.id}, '), 0))
        alter = False

        for field_name, field_data in self.field_definitions[model].items():
            related_model = field_data['related_model']

            object_field_value = getattr(object, field_name)
            if not object_field_value:
                # This field is empty; no need to map it to anything.
                self.print(indent(gray(f'Retargeting <{object._meta.object_name}>{object.id}, '
                                       f"field '{field_name}' is empty"), 1))
                continue

            nat_keys_value = self.source_data[related_model].get(object_field_value)
            if not nat_keys_value:
                raise ValueError(f'No related data found for <{object._meta.object_name}>{object.id}.{field_name}:'
                                 f' {getattr(object, field_name)} on source shard')

            mapped_value = self.get_mapped_value(related_model, nat_keys_value)
            # If the mapping is the same, this fields does not need to be translated
            if mapped_value == object_field_value:
                continue

            self.print(indent(gray(f'Retargeting <{object._meta.object_name}>{object.id}, field {field_name} to '
                                   f'{mapped_value}'), 1))
            setattr(object, field_name, mapped_value)
            alter = True

        if alter:
            object.save(update_fields=self.field_definitions[model].keys(), force_update=True)

    def retarget_relations(self):
        """
        Retargeting of foreign keys from sharded data to public data goes as follows:
        1)  Gather a list of models that have relation fields to public models.
        2)  Walk through each of these models:
            3)  Keep a list of relation fields and note down the natural key names for the related models
            4)  For each of these relations:
                5)  Gather all objects belonging to the related (public) model from the source node
                    This is saved in `source_data` and is a set with key: id and value: natural key values
                6)  Gather all objects belonging to the related (public) model from the target node
                    This is saved in `target_data` with reverse mapping. So they key is the natural-key values,
                    and the values are the ids
        7) Walk through sharded models again
            8)  For each of their objects on the target node:
                9) Read the related id
                10) Lookup the natural keys via the `source_data` dict
                11A) Lookup the new id on the target node by looking up the natural keys in the `target_node` dict
                11B) If it does not exist on the target node, copy it and go back to step 9 for that object.
                12) Write the new id to the object and save the model.

        You might reason: Why not just loop over the moved objects and use .get_natural_keys() and
        .by.natural.keys() to translate them one by one?
        Because I think this will be used to migrate millions of rows. And iterating over them and doing three
        queries for each, does not scale well.
        """
        # Select models that have a related field to a PUBLIC model
        sharded_models = [m for m in get_all_sharded_models(include_auto_created=True)
                          if any(f for f in m._meta.fields
                                 if f.is_relation
                                 and get_model_sharding_mode(self.get_related_model(f)) == ShardingMode.PUBLIC)]
        public_models = [m for m in get_all_public_models(include_auto_created=True)
                         if any(f for f in m._meta.fields
                                if f.is_relation
                                and get_model_sharding_mode(self.get_related_model(f)) == ShardingMode.PUBLIC)]

        self.field_definitions = defaultdict(lambda: defaultdict(dict))
        # <model>: {<field_name>: ('natural_keys': (nat key name 1, name 2), 'related_model': <related model name>)}
        self.source_data = defaultdict(dict)  # <model>: {<id>: (<natural key value>, natural key value 2>, etc)}
        self.target_data = defaultdict(dict)  # <model>: {(<natural key value>, natural key value 2>, etc): <id>}

        bar = progressbar.ProgressBar(
            max_value=progressbar.UnknownLength,
            widgets=[progressbar.RotatingMarker(),
                     ' Retargeting relations;',
                     progressbar.Timer()])

        # Build the source_data and target_data dicts. We also do this for public models, as might create new entries
        # that need to relate to other public models as well
        for model in sharded_models + public_models:
            self.field_definitions[model] = {}
            fields = list(f for f in model._meta.fields if f.is_relation
                          and get_model_sharding_mode(self.get_related_model(f)) == ShardingMode.PUBLIC)
            for field in fields:
                related_model = self.get_related_model(field)
                natural_keys = related_model._meta.unique_together
                if not natural_keys:
                    raise ValueError('Model {} does not appear to have natural keys!'.format(related_model))
                self.field_definitions[model][field.attname] = {'natural_keys': natural_keys[0],
                                                                'related_model': related_model}
                self.source_data[related_model] = {}
                self.target_data[related_model] = {}

            with self.source_shard.use(active_only_schemas=False, lock=False):
                for rel_model in self.source_data.keys():
                    data = rel_model.objects.all() \
                        .values_list('id', *rel_model._meta.unique_together[0])
                    self.source_data[rel_model] = {d[0]: d[1:] for d in data}

            with self.target_shard_options.use():
                for rel_model in self.target_data.keys():
                    data = rel_model.objects.all() \
                        .values_list('id', *rel_model._meta.unique_together[0])
                    self.target_data[rel_model] = {d[1:]: d[0] for d in data}

            self.bar_update(bar)

        # Use the source_data and target_data dicts to translate ids
        for model in sharded_models:
            with self.target_shard_options.use():
                # Check for all entries that need retargeting
                current_id = model.objects.order_by("pk")[0].pk  # Lowest id
                end_id = model.objects.order_by("-pk")[0].pk  # Highest id

                while current_id <= end_id:
                    object_batch = list(model.objects.filter(pk__gte=current_id, pk__lt=current_id+self.batch_size)
                                        .only(*list(self.field_definitions[model].keys()) + ['pk']))

                    for obj in object_batch:
                        self._check_relations(model, obj)
                        self.bar_update(bar)

                    current_id += self.batch_size

        self.bar_finish(bar)

    def reset_sequences(self):
        """
        Reset the sequencers for all models on the target schema
        """
        with self.target_shard_options.use() as env:
            env.connection.reset_sequence(model_list=get_all_sharded_models())

    def post_execution(self, succeeded):
        """
        Called after the transaction is committed. Both after success or failure.
        Set shard back to their original state.
        Will update the shard's new node living accommodation if the migration was successful
        """
        source_connection = connections[self.source_shard.node_name]
        mapping_model = get_mapping_class()

        if mapping_model:
            # No need to fetch the objects from the shards, because we can use the old_source_state dictionary here to
            # release the locks and put the mapping objects back to their old state.
            for root_object_id, state in self.old_source_states.items():
                mapping_object = mapping_model.objects.get(id=root_object_id)
                mapping_object.state = state
                mapping_object.save(update_fields=['state'])

                # Release the exclusive advisory lock
                source_connection.release_advisory_lock(key='mapping_{}'.format(root_object_id), shared=False)

        # Restore shard state and set its node to the target
        source_connection.release_advisory_lock(key='shard_{}'.format(self.source_shard.id), shared=False)

        self.source_shard.state = self.old_shard_state
        if succeeded:
            self.source_shard.node_name = self.target_node
        self.source_shard.save(update_fields=['state', 'node_name'])

    @staticmethod
    def copy_expert(cursor, *args, **kwargs):
        """
        Make this mockable by giving it a separate function.
        """
        cursor.copy_expert(*args, **kwargs)
