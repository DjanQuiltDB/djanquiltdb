"""
Checks the plugin registers with Django's system check framework, from ``install()``.
"""

from django.conf import settings
from django.core import checks
from djanquiltdb import ShardingMode
from djanquiltdb.utils import placement_of_sharding_mode_value, resolve_node_alias
from postgres_objects.base import DeclarativeObject
from postgres_objects.registry import get_declared_objects, get_functions_module, get_views_module

_NO_PLACEMENT_HINT = (
    'It gets the default placement, the public schema of every node, without saying so. Decorate it with one of the '
    'placement decorators, such as @public_function() or @sharded_view(). To keep it on one node at your own risk, use '
    '@single_node_unsharded_function(node) or @single_node_unsharded_view(node).'
)

_SINGLE_NODE_HINT = (
    'A database object on a single node is strongly discouraged: only that node has it. Silence this warning with '
    "SILENCED_SYSTEM_CHECKS = ['djanquiltdb_plugin_postgres_objects.W001']."
)

_NO_NODE_HINT = (
    'Decorate it with @single_node_unsharded_function(node) or @single_node_unsharded_view(node), which name the node, '
    "or give its router_hints a 'node'."
)


def _declarations(app_configs):
    """
    Returns the declared functions and views of the apps in ``app_configs``, or of every app when it is None.
    """
    app_labels = None if app_configs is None else {app_config.label for app_config in app_configs}
    declarations = []

    for module_path in (get_functions_module(), get_views_module()):
        if module_path is None:
            continue

        for (app_label, _), declaration in sorted(get_declared_objects(module_path).items()):
            if app_labels is None or app_label in app_labels:
                declarations.append(declaration)

    return declarations


def has_placement_decorator(declaration):
    """
    Whether a placement decorator sets the ``router_hints`` of ``declaration``, or of a declaration that it subclasses,
    rather than the default placement that ``install()`` gives ``DeclarativeObject``.
    """
    return any('router_hints' in vars(cls) for cls in declaration.__mro__ if cls is not DeclarativeObject)


@checks.register(checks.Tags.models)
def check_declarations_have_a_placement(app_configs, **kwargs):
    """
    Report each declared function and view without a placement decorator, and warn about each one on a single node.
    When ``app_configs`` is given, only the declarations of those apps are checked.
    """
    messages = []

    for declaration in _declarations(app_configs):
        label = '{}.{}'.format(declaration.resolved_app_label, declaration.__name__)

        if not has_placement_decorator(declaration):
            messages.append(
                checks.Error(
                    'The database object {} has no placement decorator.'.format(label),
                    hint=_NO_PLACEMENT_HINT,
                    obj=declaration,
                    id='djanquiltdb_plugin_postgres_objects.E001',
                )
            )
            continue

        # The router reads the node from the sharding mode, as single_node_unsharded(node) gives it, or from the 'node'
        # hint, as the single-node decorators give it.
        sharding_mode, node = placement_of_sharding_mode_value(declaration.router_hints.get('sharding_mode'))
        node = declaration.router_hints.get('node') if node is None else node

        if sharding_mode is not ShardingMode.SINGLE_NODE_UNSHARDED:
            continue

        if node is None:
            messages.append(
                checks.Error(
                    'The database object {} is on a single node, but its router_hints name no node.'.format(label),
                    hint=_NO_NODE_HINT,
                    obj=declaration,
                    id='djanquiltdb_plugin_postgres_objects.E003',
                )
            )
            continue

        single_node_alias = resolve_node_alias(node)
        messages.append(
            checks.Warning(
                "The database object {} is on the single node '{}'.".format(label, single_node_alias),
                hint=_SINGLE_NODE_HINT,
                obj=declaration,
                id='djanquiltdb_plugin_postgres_objects.W001',
            )
        )

        if single_node_alias not in settings.DATABASES:
            messages.append(
                checks.Error(
                    "The database object {} is on the node '{}', which is not in DATABASES.".format(
                        label, single_node_alias
                    ),
                    obj=declaration,
                    id='djanquiltdb_plugin_postgres_objects.E002',
                )
            )

    return messages
