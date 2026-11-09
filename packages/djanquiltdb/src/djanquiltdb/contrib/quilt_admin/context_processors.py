import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError
from django.utils.module_loading import import_string

from djanquiltdb.contrib.quilt_admin import apps as quilt_admin_apps
from djanquiltdb.contrib.quilt_admin.utils import list_switchable_mappings, list_switchable_shards
from djanquiltdb.utils import get_mapping_class, get_shard_class

logger = logging.getLogger(__name__)


def _case_insensitive_sort_key(value):
    """
    Returns a sort key that ignores the case of a string. Other values are compared as they are, and None comes last.
    """
    return (value is None, value.casefold() if isinstance(value, str) else value)


def mapping_value_sort_key(value, shard, mapping):
    """Returns the sort key of the mapping value, or of the shard alias when the project has no mapping model."""
    if mapping is None:
        return shard_alias_sort_key(value, shard, mapping)
    return _case_insensitive_sort_key(value)


def shard_alias_sort_key(value, shard, mapping):
    return _case_insensitive_sort_key(shard.alias if shard is not None else None)


def schema_name_sort_key(value, shard, mapping):
    return _case_insensitive_sort_key(shard.schema_name if shard is not None else None)


BUILT_IN_SORT_FUNCTIONS = {
    'mapping_value': mapping_value_sort_key,
    'shard_alias': shard_alias_sort_key,
    'schema_name': schema_name_sort_key,
}


def get_shard_switcher_sort_function():
    """
    Returns the sort function that QUILT_ADMIN['SHARD_SWITCHER_SORT_KEY'] sets. Raises ImproperlyConfigured when the
    setting is not a built-in choice, a dotted path or a function.
    """
    setting_value = getattr(settings, 'QUILT_ADMIN', {}).get('SHARD_SWITCHER_SORT_KEY', 'mapping_value')

    if callable(setting_value):
        return setting_value
    if isinstance(setting_value, str) and setting_value in BUILT_IN_SORT_FUNCTIONS:
        return BUILT_IN_SORT_FUNCTIONS[setting_value]
    if isinstance(setting_value, str) and '.' in setting_value:
        return import_string(setting_value)

    raise ImproperlyConfigured(
        "QUILT_ADMIN['SHARD_SWITCHER_SORT_KEY'] is {!r}. Set it to one of {}, the dotted path of a function, or a "
        'function.'.format(setting_value, ', '.join(repr(name) for name in BUILT_IN_SORT_FUNCTIONS))
    )


def admin_shard_context(request):
    """
    Context processor to provide shard information to admin templates.
    Adds shard switcher context to admin templates.

    If a mapping model is configured (QUILT_DB["MAPPING_MODEL"]), the selector lists
    mapping entries (by mapping value) instead of shards (by shard id). The entries are limited to the mappings that
    switchable_mappings() of the shard selector class returns, or to the shards that switchable_shards() returns.
    QUILT_ADMIN['SHARD_SWITCHER_SORT_KEY'] decides the order of the entries.
    """
    context = {
        'available_shards': [],
        'current_shard_id': None,  # kept for backwards compatibility
        'shard_switcher_options': [],
        'shard_switcher_field_name': 'shard_id',
        'current_shard_switcher_value': '',
        'shard_switcher_mode': 'shard',  # or 'mapping'
        'shard_maintenance_mode': False,
        'shard_maintenance_message': None,
        'use_csp_nonce': getattr(settings, 'QUILT_ADMIN', {}).get('USE_CSP_NONCE', False),
    }
    # Add maintenance status if available
    if hasattr(request, '_shard_maintenance_mode'):
        context['shard_maintenance_mode'] = request._shard_maintenance_mode
        context['shard_maintenance_message'] = getattr(request, '_shard_maintenance_message', None)
    # Only add shard context if user is authenticated and in admin
    if hasattr(request, 'user') and request.user.is_authenticated and request.path.startswith('/admin/'):
        sort_function = get_shard_switcher_sort_function()
        try:
            mapping_class = get_mapping_class()
            if mapping_class:
                mapping_field = getattr(mapping_class, 'mapping_field', None)

                # The database order only decides between options that have the same sort key.
                entries = []
                for mapping in list_switchable_mappings(request, mapping_class):
                    mapping_value = getattr(mapping, mapping_field) if mapping_field else mapping.pk
                    entries.append((mapping_value, getattr(mapping, 'shard', None), mapping))
                entries.sort(key=lambda entry: sort_function(*entry))

                context['available_shards'] = [mapping for _, _, mapping in entries]
                context['shard_switcher_mode'] = 'mapping'
                context['shard_switcher_field_name'] = 'mapping_value'

                current_value = quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request)
                context['current_shard_switcher_value'] = '' if current_value is None else str(current_value)

                context['shard_switcher_options'] = [
                    {
                        'value': '' if mapping_value is None else str(mapping_value),
                        'label': quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.format_override_option(
                            mapping_value, shard, mapping
                        ),
                    }
                    for mapping_value, shard, mapping in entries
                ]
            else:
                shards = list_switchable_shards(request, get_shard_class())
                shards.sort(key=lambda shard: sort_function(shard.id, shard, None))
                context['available_shards'] = shards

                current_id = quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request)
                context['current_shard_id'] = current_id
                context['current_shard_switcher_value'] = '' if current_id is None else str(current_id)

                context['shard_switcher_options'] = [
                    {
                        'value': str(shard.id),
                        'label': quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.format_override_option(shard.id, shard),
                    }
                    for shard in shards
                ]
        except DatabaseError:
            # The database may not be ready (fresh deploy, migrations pending); degrade to an empty switcher, but leave
            # a trace instead of hiding the failure entirely.
            logger.warning('Could not build the admin shard switcher context.', exc_info=True)

    return context
