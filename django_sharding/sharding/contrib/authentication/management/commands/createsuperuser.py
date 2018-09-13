from contextlib import contextmanager

from django.contrib.auth.management.commands.createsuperuser import Command as CreateSuperUserCommand
from django.core.management import CommandError

from sharding import ShardingMode
from sharding.decorators import atomic_write_to_every_node
from sharding.utils import get_model_sharding_mode, get_all_databases, use_shard, get_shard_class, for_each_node


class BaseManagerMixin:
    def get_by_natural_key(self, *args, **kwargs):
        """
        The `createsuperuser` command check for uniqueness of the username by calling this method with the specified
        username. If the username exists, this will return a `model.DoesNotExist` and the command will continue.
        However, since we query this on each node, we need to make sure that ALL the nodes return a
        `model.DoesNotExist`. That's why we return `False` in case the model does not exist on a single node. If all
        nodes return `False`, then we do raise the model.DoesNotExists. If a single node returns `True`, then we
        know that the username exists on one node, and we return `None` (regardless of the other databases), which makes
        sure that the command will show a nice error to the user telling that the username already exists.
        """
        if all(x is False for x in for_each_node(self._get_by_natural_key, args=args, kwargs=kwargs).values()):
            raise self.model.DoesNotExist()

    def _get_by_natural_key(self, *args, node_name=None, **kwargs):
        with use_shard(node_name=node_name, schema_name='public'):
            try:
                return super().get_by_natural_key(*args, **kwargs)
            except self.model.DoesNotExist:
                return False

    @atomic_write_to_every_node()
    def create_superuser(self, *args, node_name=None, **kwargs):
        """
        Makes sure that this method will be called on each node's public schema.
        """
        super().create_superuser(*args, **kwargs)


@contextmanager
def patch_user_manager(model):
    """
    Contextmanager that patches the model's `get_by_natural_key` and `create_superuser` to perform the operation on each
    node. Will return to the old default manager after leaving the contextmanager.
    """
    default_manager = model._default_manager
    manager_class = default_manager.__class__

    new_manager = type(
        manager_class.__name__,
        (BaseManagerMixin, manager_class.from_queryset(default_manager._queryset_class)),
        {}
    )()

    new_manager.contribute_to_class(model, '_create_superuser_manager')
    model._default_manager = new_manager

    try:
        yield model
    finally:
        # And do some cleanup, to make sure we can continue with other code if we call this command with
        # `call_command`
        model._default_manager = default_manager


class Command(CreateSuperUserCommand):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_sharding_mode = get_model_sharding_mode(self.UserModel)

    def add_arguments(self, parser):
        super().add_arguments(parser)
        if self.user_sharding_mode == ShardingMode.SHARDED:
            parser._option_string_actions['--database'].help = 'Specifies the database to use. '
            parser._option_string_actions['--database'].default = None
            parser._option_string_actions['--database'].required = True
            parser._option_string_actions['--database'].choices = sorted(get_all_databases())
            parser.add_argument(
                '--schema-name',
                required=True,
                help='Specifies the schema to use.'
            )
        else:
            parser._option_string_actions['--database'].help = \
                'Specifies the database to use. Defaults to all databases.'
            parser._option_string_actions['--database'].default = 'all'
            parser._option_string_actions['--database'].choices = ['all'] + sorted(get_all_databases())

    def handle(self, *args, **options):
        if self.user_sharding_mode == ShardingMode.SHARDED:
            # We can safely do the .filter().first(), because node_name and schema_name are unique together. So it
            # either returns `None` or a shard.
            shard = get_shard_class().objects \
                .filter(node_name=options['database'], schema_name=options['schema_name']) \
                .first()

            if not shard:
                raise CommandError('The shard you provided ({}|{}) does not exist'.format(options['database'],
                                                                                          options['schema_name']))

            with use_shard(shard):
                super().handle(*args, **options)

        if self.user_sharding_mode == ShardingMode.MIRRORED:
            if options['database'] != 'all':
                with use_shard(node_name=options['database'], schema_name='public'):
                    super().handle(*args, **options)
            else:
                self.handle_all_databases(*args, **options)

    def handle_all_databases(self, *args, **options):
        """
        This is a special case, since we don't want to prompt the user to fill in their username and password again for
        each database. We want the same user on each database. So we're going to change the UserModel's default manager,
        and make sure we run the `create_superuser` method on each node.

        This is a separate method, which allows the user to override it in their own application. In case they have for
        example already something built in their own manager that is already doing a fanout to each node.
        """
        with patch_user_manager(self.UserModel):
            options['database'] = None
            super().handle(*args, **options)
