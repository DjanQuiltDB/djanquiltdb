from django.http import HttpResponse
from django.urls import resolve

from djanquiltdb import State
from djanquiltdb.contrib.quilt_admin.apps import ADMIN_SHARD_SELECTOR_CLASS
from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy
from djanquiltdb.middleware import BaseUseShardForMiddleware, BaseUseShardMiddleware
from djanquiltdb.utils import get_mapping_class, get_shard_class, use_shard, use_shard_for

"""
Middleware classes that allow viewing the admin for a shard different from the one that your user is stored on. These
should be configured alongside, not instead of, a standard BaseUseShardMiddleware subclass (e.g. UseShardForMiddleware),
and should always be configured below both that middleware and AuthenticationMiddleware, since the request.user object
needs to be populated (and authenticated) on its own shard before the switching can occur.
"""


def _is_shard_switching_request(request):
    """
    Check if the request is for switching shards (should be allowed even in maintenance mode).
    """
    try:
        resolved = resolve(request.path)
        return resolved.url_name == 'djanquiltdb_switch_shard'
    except Exception:
        return False


def _is_logout_request(request):
    """
    Check if the request is for logout (should be allowed even in maintenance mode).
    """
    try:
        resolved = resolve(request.path)
        return resolved.url_name == 'logout' or resolved.url_name == 'admin:logout'
    except Exception:
        # Fallback to path check if URL resolution fails
        return request.path.endswith('/logout/') or '/logout' in request.path


def _check_maintenance_status(request, shard_id=None, mapping_value=None):
    """
    Check if the current shard or mapping entry is in maintenance mode.
    Stores the maintenance status on the request object for use in templates.

    Returns True if in maintenance, False otherwise.
    """
    is_maintenance = False
    maintenance_message = None

    if shard_id:
        shard = get_shard_class().objects.using('default').get(id=shard_id)
        if shard.state == State.MAINTENANCE:
            is_maintenance = True
            maintenance_message = 'This shard is currently in maintenance mode.'

    if mapping_value:
        mapping_class = get_mapping_class()
        if mapping_class:
            mapping_field = getattr(mapping_class, 'mapping_field', None)
            if mapping_field:
                try:
                    mapping_obj = (
                        mapping_class.objects.using('default')
                        .select_related('shard')
                        .filter(**{mapping_field: mapping_value})
                        .first()
                    )
                    if mapping_obj:
                        if mapping_obj.state == State.MAINTENANCE:
                            is_maintenance = True
                            maintenance_message = 'This mapping entry is currently in maintenance mode.'
                        # Also check shard state (check independently)
                        # Ensure shard is loaded - if select_related didn't work, fetch it explicitly
                        shard = getattr(mapping_obj, 'shard', None)
                        if not shard and hasattr(mapping_obj, 'shard_id') and mapping_obj.shard_id:
                            # Shard wasn't loaded, fetch it
                            try:
                                shard = get_shard_class().objects.using('default').get(id=mapping_obj.shard_id)
                            except Exception:
                                shard = None
                        # If shard still not available, try to access it directly (might trigger a query)
                        if not shard:
                            try:
                                shard = mapping_obj.shard
                            except Exception:
                                pass
                        if shard and shard.state == State.MAINTENANCE:
                            is_maintenance = True
                            maintenance_message = 'This shard is currently in maintenance mode.'
                except Exception:
                    pass

    # Store maintenance status on request for use in templates
    # Always set these attributes so templates can check them
    request._shard_maintenance_mode = is_maintenance
    request._shard_maintenance_message = maintenance_message if is_maintenance else None

    return is_maintenance


class BaseAdminOverrideUseShardMiddleware(BaseUseShardMiddleware):
    def process_request(self, request):
        # Return early if the request is not for /admin
        if not request.path.startswith('/admin/'):
            return None

        # If we are viewing a different shard than the user's own, ensure request.user is proxied.
        # Otherwise, ensure it is not/no longer proxied.
        if shard_id := self.get_shard_id(request):
            if request.user is not None and not isinstance(request.user, CrossShardUserProxy):
                request.user = CrossShardUserProxy(
                    request.user,
                    ADMIN_SHARD_SELECTOR_CLASS.retrieve_main_value(request),
                )
        else:
            if request.user is not None and isinstance(request.user, CrossShardUserProxy):
                request.user = request.user._user

        if result := super().process_request(request):
            return result

        # Check maintenance status for both GET and POST requests
        is_maintenance = _check_maintenance_status(request, shard_id=shard_id)

        # Block POST requests if in maintenance (except shard switching and logout)
        # Only apply this check for admin requests
        if (
            request.method == 'POST'
            and not _is_shard_switching_request(request)
            and not _is_logout_request(request)
            and is_maintenance
        ):
            return HttpResponse(request._shard_maintenance_message + ' Changes are not allowed.', status=503)

        return result

    def _enable_shard(self, request, shard_id):
        # Allow access to maintenance shards for the admin (but read-only)
        shard = get_shard_class().objects.get(id=shard_id)
        shard_context_manager = self.set_shard_context_manager(request, use_shard(shard, active_only_schemas=False))
        shard_context_manager.enable()


class BaseAdminOverrideUseShardForMiddleware(BaseUseShardForMiddleware):
    def process_request(self, request):
        # Return early if the request is not for /admin
        if not request.path.startswith('/admin/'):
            return None

        # If we are viewing a different shard than the user's own, ensure request.user is proxied.
        # Otherwise, ensure it is not/no longer proxied.
        if mapping_value := self.get_mapping_value(request):
            if request.user is not None and not isinstance(request.user, CrossShardMappingUserProxy):
                request.user = CrossShardMappingUserProxy(
                    request.user,
                    ADMIN_SHARD_SELECTOR_CLASS.retrieve_main_value(request),
                )
        else:
            if request.user is not None and isinstance(request.user, CrossShardMappingUserProxy):
                request.user = request.user._user

        if result := super().process_request(request):
            return result

        # Check maintenance status for both GET and POST requests
        is_maintenance = _check_maintenance_status(request, mapping_value=mapping_value)

        # Block POST requests if in maintenance (except shard switching and logout)
        # Only apply this check for admin requests
        if (
            request.method == 'POST'
            and not _is_shard_switching_request(request)
            and not _is_logout_request(request)
            and is_maintenance
        ):
            return HttpResponse(request._shard_maintenance_message + ' Changes are not allowed.', status=503)

        return result

    def _enable_shard_for(self, request, target_value):
        # Allow access to maintenance shards for the admin (but read-only)
        shard_context_manager = self.set_shard_context_manager(
            request, use_shard_for(target_value, active_only_schemas=False)
        )
        shard_context_manager.enable()


class ShardIdAdminOverrideMiddleware(BaseAdminOverrideUseShardMiddleware):
    def get_shard_id(self, request):
        return request.session.get(ADMIN_SHARD_SELECTOR_CLASS.override_shard_selector_key, None)


class MappingValueAdminOverrideMiddleware(BaseAdminOverrideUseShardForMiddleware):
    def get_mapping_value(self, request):
        return request.session.get(ADMIN_SHARD_SELECTOR_CLASS.override_shard_selector_key, None)
