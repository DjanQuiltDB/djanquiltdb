from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import models
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views import View

from djanquiltdb.contrib.quilt_admin.apps import ADMIN_SHARD_SELECTOR_CLASS
from djanquiltdb.utils import get_mapping_class, get_shard_class


def _coerce_value(raw_value, field):
    """
    Convert a POSTed string value into the Python type for a model field.
    Falls back to the raw string if coercion fails or the field type is unknown.
    """
    if raw_value is None:
        return None

    try:
        if isinstance(field, (models.AutoField, models.BigAutoField, models.IntegerField, models.BigIntegerField)):
            return int(raw_value)
    except (TypeError, ValueError):
        return raw_value

    return raw_value


@method_decorator(login_required, name='dispatch')
class SwitchShardView(View):
    """
    View to handle shard switching from the admin dropdown.
    """

    def post(self, request):
        mapping_class = get_mapping_class()

        if mapping_class:
            raw_value = request.POST.get('mapping_value')
            if raw_value:
                mapping_field = getattr(mapping_class, 'mapping_field', None)
                if not mapping_field:
                    messages.error(
                        request,
                        'Mapping model is configured, but it does not define a mapping_field. '
                        'Did you decorate it with @shard_mapping_model(...) ?',
                    )
                else:
                    field = mapping_class._meta.get_field(mapping_field)
                    mapping_value = _coerce_value(raw_value, field)

                    mapping_obj = (
                        mapping_class.objects.using('default')
                        .select_related('shard')
                        .filter(**{mapping_field: mapping_value})
                        .first()
                    )
                    if not mapping_obj:
                        messages.error(request, 'Selected mapping value does not exist.')
                    else:
                        ADMIN_SHARD_SELECTOR_CLASS.set_override_value(request, mapping_value)
                        shard = getattr(mapping_obj, 'shard', None)
                        if shard is not None:
                            messages.success(
                                request,
                                f'Switched to mapping: {mapping_value} (shard: {shard.alias})',
                            )
                        else:
                            messages.success(request, f'Switched to mapping: {mapping_value}')
            else:
                # Clear override
                ADMIN_SHARD_SELECTOR_CLASS.set_override_value(request, None)
                messages.success(request, 'Cleared override; using default routing.')
        else:
            shard_id = request.POST.get('shard_id')
            if shard_id:
                try:
                    shard_class = get_shard_class()
                    shard = shard_class.objects.using('default').get(id=shard_id)
                    ADMIN_SHARD_SELECTOR_CLASS.set_override_value(request, shard_id)
                    messages.success(request, f'Switched to shard: {shard.alias}')
                except (ValueError, TypeError):
                    messages.error(request, 'Selected shard id is invalid.')
                except shard_class.DoesNotExist:
                    messages.error(request, 'Selected shard does not exist.')
            else:
                # Clear override
                ADMIN_SHARD_SELECTOR_CLASS.set_override_value(request, None)
                messages.success(request, 'Cleared override; using default routing.')

        # Redirect back to the referring page or admin index
        redirect_url = request.POST.get('next', request.META.get('HTTP_REFERER', reverse('admin:index')))
        return HttpResponseRedirect(redirect_url)
