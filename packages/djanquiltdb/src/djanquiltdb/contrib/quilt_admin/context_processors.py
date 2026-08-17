from django.conf import settings

from djanquiltdb.contrib.quilt_admin.apps import ADMIN_SHARD_SELECTOR_CLASS
from djanquiltdb.utils import get_mapping_class, get_shard_class


def admin_shard_context(request):
    """
    Context processor to provide shard information to admin templates.
    Adds shard switcher context to admin templates.

    If a mapping model is configured (QUILT_DB["MAPPING_MODEL"]), the selector lists
    mapping entries (by mapping value) instead of shards (by shard id).
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
        try:
            mapping_class = get_mapping_class()
            if mapping_class:
                mapping_field = getattr(mapping_class, 'mapping_field', None)

                qs = mapping_class.objects.using('default').select_related('shard').all()
                if mapping_field:
                    qs = qs.order_by(mapping_field)
                else:
                    qs = qs.order_by('pk')

                mappings = list(qs)
                context['available_shards'] = mappings
                context['shard_switcher_mode'] = 'mapping'
                context['shard_switcher_field_name'] = 'mapping_value'

                current_value = ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request)
                context['current_shard_switcher_value'] = '' if current_value is None else str(current_value)

                options = []
                for mapping in mappings:
                    if mapping_field:
                        mapping_value = getattr(mapping, mapping_field)
                        mapping_value_str = '' if mapping_value is None else str(mapping_value)
                    else:
                        mapping_value = mapping.pk
                        mapping_value_str = str(mapping.pk)

                    shard = getattr(mapping, 'shard', None)
                    label = ADMIN_SHARD_SELECTOR_CLASS.format_override_option(mapping_value, shard, mapping)

                    options.append({'value': mapping_value_str, 'label': label})

                context['shard_switcher_options'] = options
            else:
                shard_class = get_shard_class()
                # Get all shards from the primary database
                # Using .using('default') to ensure we query from the primary database
                shards = list(shard_class.objects.using('default').all().order_by('alias'))
                context['available_shards'] = shards

                current_id = ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request)
                context['current_shard_id'] = current_id
                context['current_shard_switcher_value'] = '' if current_id is None else str(current_id)

                context['shard_switcher_options'] = [
                    {
                        'value': str(shard.id),
                        'label': ADMIN_SHARD_SELECTOR_CLASS.format_override_option(shard.id, shard),
                    }
                    for shard in shards
                ]
        except Exception:
            # If there's any error (e.g., database not ready, no shards exist), just return empty context
            pass

    return context
