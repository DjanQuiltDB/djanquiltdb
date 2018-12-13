from django.contrib.admin.utils import NestedObjects
from django.core.exceptions import FieldError, MultipleObjectsReturned
from django.core.management import BaseCommand, CommandError
from django.db import connections
from django.db.models import ObjectDoesNotExist
from django.utils import termcolors

from sharding import ShardingMode
from sharding.apps import apps
from sharding.collector import SimpleCollector
from sharding.options import ShardOptions
from sharding.utils import get_shard_class, get_all_sharded_models, get_model_sharding_mode


class Command(BaseCommand):
    help = 'Purge all data belonging to an object on a specific shard.'

    shard, options = None, {}

    def __init__(self, stdout=None, stderr=None, no_color=False):
        super().__init__(stdout=stdout, stderr=stderr, no_color=no_color)

        if no_color:
            self.style.ACCENT = termcolors.make_style()
            self.style.BOLD = termcolors.make_style()
        else:
            self.style.ACCENT = termcolors.make_style(fg='magenta')
            self.style.BOLD = termcolors.make_style(opts=('bold',))

    def add_arguments(self, parser):
        parser.add_argument(dest='shard_alias',
                            help='Name of the shard for which to purge the data.')
        parser.add_argument(dest='model_name',
                            help='Dot notation of the module path to the root object model class, e.g. '
                                 '"app_label.model_name".')
        parser.add_argument(dest='object_value',
                            help='The object value for the object field.')
        parser.add_argument('--object-field',
                            action='store',
                            dest='object_field',
                            help="The field to map the object object value to. Defaults to 'id'.",
                            default='id')
        parser.add_argument('--simple-collector',
                            action='store_true',
                            dest='simple_collector',
                            help="Do not use Django's delete collector to determine what needs to be deleted from the "
                                 "shard, but use the simple collector.",
                            default=False)
        parser.add_argument('--noinput',
                            action='store_false',
                            dest='interactive',
                            help='Do NOT prompt the user for input of any kind and assume "yes" on all questions.',
                            default=True)

    def log(self, msg, level=2):
        if self.options['verbosity'] >= level:
            self.stdout.write(msg)

    def handle(self, *args, **options):
        self.options = options

        self.shard = self.get_shard(alias=self.options['shard_alias'])

        self.log('\nGathering data:\n')

        collector = self.get_data_collector(
            objects=self.get_objects(),
            use_original_collector=not self.options['simple_collector'],
        )

        if not self.confirm(collector.data):
            self.log('\nOperation cancelled.', level=1)
            return

        self.delete_data(collector=collector)

        self.log('\nDone. Deleted {} data points'.format(sum(map(len, collector.data.values()))))

    def confirm(self, data):
        for model, instances in data.items():
            self.log(self.style.ACCENT(model))
            self.log('\t{} data points'.format(len(instances)))

        if self.options['interactive']:
            confirm_msg = \
                "\nYou have requested to purge all data for object with object value\n{} on shard {}.\nThis " \
                "will IRREVERSIBLY DESTROY all data for this object on the given shard.\n" \
                "Are you sure you want to do this?\n" \
                "\n\tType 'yes' to continue, or 'no' to cancel: ".format(
                    self.style.BOLD(self.options['object_value']), self.style.BOLD(self.shard)
                )

            confirm = input(confirm_msg)

            if confirm != 'yes':
                return False

        return True

    def get_objects(self):
        model = apps.get_model(self.options['model_name'])

        if not get_model_sharding_mode(model) == ShardingMode.SHARDED:
            raise CommandError("'{}' is not a sharded model.".format(self.options['model_name']))

        using = ShardOptions.from_shard(shard=self.shard, active_only_schemas=False)
        fmt = {
            'object_field': self.options['object_field'],
            'object_value': self.options['object_value'],
            'model_name': self.options['model_name'],
        }  # Shortcut

        try:
            return [model.objects.using(using).get(**{self.options['object_field']: self.options['object_value']})]
        except ValueError:
            raise CommandError(
                "Provided object value '{object_value}' is invalid for object field '{object_field}' for model "
                "'{model_name}'.".format(**fmt)
            )
        except FieldError:
            raise CommandError(
                "Object field '{object_field}' is not valid for model '{model_name}'.".format(**fmt)
            )
        except ObjectDoesNotExist:
            raise CommandError(
                "No object could be found with object field '{object_field}' and object value "
                "'{object_value}' for model '{model_name}'.".format(**fmt)
            )
        except MultipleObjectsReturned:
            raise CommandError(
                "Multiple objects found with object field '{object_field}' and object value '{object_value}' for model "
                "'{model_name}'.".format(**fmt)
            )

    @staticmethod
    def get_shard(alias):
        """
        Get shard based on alias.
        """
        shard = get_shard_class().objects.filter(alias=alias).first()

        if not shard:
            raise CommandError("No shard could be found with alias '{}'".format(alias))

        return shard

    def get_data_collector(self, objects, use_original_collector=False):
        sharded_models = get_all_sharded_models(include_auto_created=True)

        if use_original_collector:
            collector = NestedObjects(
                using=ShardOptions.from_shard(
                    shard=self.shard,
                    active_only_schemas=False,  # Do not raise exception if the schema is inactive
                    lock=False,  # We're selecting stale and unused data so no locking required
                )
            )
        else:
            collector = SimpleCollector(connection=connections[self.shard], verbose=bool(self.options['verbosity']))

        # Collect the data
        collector.collect(objects)

        # Stop execution if we collected data for mirrored models
        if any((model not in sharded_models for model in list(collector.data.keys()))):
            raise CommandError(
                'There might be something wrong with your data structure, because the collector '
                'collected mirrored models. Check your data points closely to see if no unexpected '
                'model instances are collected.',
            )

        return collector

    def delete_data(self, collector):
        collector.delete()
