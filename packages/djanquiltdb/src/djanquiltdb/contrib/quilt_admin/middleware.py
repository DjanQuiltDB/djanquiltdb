from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.urls import Resolver404, resolve

from djanquiltdb import State
from djanquiltdb.contrib.quilt_admin import apps as quilt_admin_apps
from djanquiltdb.contrib.quilt_admin.utils import (
    CrossShardMappingUserProxy,
    CrossShardUserProxy,
    find_switchable_mapping,
    find_switchable_shard,
)
from djanquiltdb.middleware import BaseUseShardForMiddleware, BaseUseShardMiddleware
from djanquiltdb.utils import (
    get_mapping_class,
    get_shard_class,
    public_schema_alias_for_reading,
    use_shard,
)

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
    except Resolver404:
        return False


def _is_logout_request(request):
    """
    Check if the request is for logout (should be allowed even in maintenance mode).
    """
    try:
        resolved = resolve(request.path)
        return resolved.url_name == 'logout' or resolved.url_name == 'admin:logout'
    except Resolver404:
        # Fallback to path check if URL resolution fails
        return request.path.endswith('/logout/') or '/logout' in request.path


def _unwrap_the_cross_shard_proxy(request):
    """Puts the logged-in user on ``request`` in place of a cross-shard proxy."""
    if isinstance(request.user, (CrossShardUserProxy, CrossShardMappingUserProxy)):
        request.user = request.user._user


def _user_is_authenticated(request):
    user = getattr(request, 'user', None)
    return user is not None and user.is_authenticated


def _refuse_an_override_that_is_not_switchable(request, override_value, message):
    """
    Handles an override that exists but that switchable_mappings() or switchable_shards() of the shard selector class
    does not return for the user of ``request``.

    A request to switch shards or to log out goes on without the override, so that the user can pick another tenant or
    log out. Any other request raises PermissionDenied, which Django turns into a 403 response, so that a request for a
    tenant that the user may not switch to is never served in the user's own shard. When the session has this override,
    the override is cleared and ``message`` tells the user so on the next page.
    """
    if _is_shard_switching_request(request) or _is_logout_request(request):
        return
    selector_class = quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS
    if override_value == selector_class.retrieve_override_value(request):
        selector_class.set_override_value(request, None)
        # The message is dropped when MessageMiddleware comes after this middleware, so that the 403 response still
        # goes out.
        messages.error(request, message, fail_silently=True)
    raise PermissionDenied


def _check_maintenance_status(request, shard_id=None, mapping_value=None):
    """
    Check if the current shard or mapping entry is in maintenance mode.
    Stores the maintenance status on the request object for use in templates.

    Returns True if in maintenance, False otherwise.
    """
    is_maintenance = False
    maintenance_message = None

    if shard_id:
        # A stale session override may point at a shard that no longer exists; the admin then simply is not
        # in maintenance, so the override can be cleared through the switcher.
        shard_class = get_shard_class()
        shard = shard_class.objects.using(public_schema_alias_for_reading(shard_class)).filter(id=shard_id).first()
        if shard is not None and shard.state == State.MAINTENANCE:
            is_maintenance = True
            maintenance_message = 'This shard is currently in maintenance mode.'

    if mapping_value:
        mapping_class = get_mapping_class()
        if mapping_class:
            mapping_field = getattr(mapping_class, 'mapping_field', None)
            if mapping_field:
                mapping_obj = (
                    mapping_class.objects.using(public_schema_alias_for_reading(mapping_class))
                    .select_related('shard')
                    .filter(**{mapping_field: mapping_value})
                    .first()
                )
                if mapping_obj:
                    if mapping_obj.state == State.MAINTENANCE:
                        is_maintenance = True
                        maintenance_message = 'This mapping entry is currently in maintenance mode.'
                    shard = mapping_obj.shard
                    if shard is not None and shard.state == State.MAINTENANCE:
                        is_maintenance = True
                        maintenance_message = 'This shard is currently in maintenance mode.'

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

        # _enable_shard() puts the cross-shard proxy on the request once it enters the shard of the override.
        _unwrap_the_cross_shard_proxy(request)

        if not _user_is_authenticated(request):
            # The admin sends the user to the login page. The override stays in the session for the next login.
            self.set_shard_context_manager(request, None)
            request._shard_id = None
            _check_maintenance_status(request)
            return None

        if result := super().process_request(request):
            return result

        # Check maintenance status for both GET and POST requests
        is_maintenance = _check_maintenance_status(request, shard_id=request._shard_id)

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
        try:
            shard = find_switchable_shard(request, shard_id)
        except PermissionDenied:
            request._shard_id = None
            _refuse_an_override_that_is_not_switchable(
                request, shard_id, 'Cleared the override, because you do not have access to the selected shard.'
            )
            return
        if shard is None:
            # The request goes on without an override when the shard registry does not have the shard of the override,
            # so that the user can clear the override through the shard switcher.
            request._shard_id = None
            return
        request.user = CrossShardUserProxy(
            request.user, quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_main_value(request)
        )
        # Allow access to maintenance shards for the admin (but read-only)
        self.set_shard_context_manager(request, use_shard(shard, active_only_schemas=False)).enable()


class BaseAdminOverrideUseShardForMiddleware(BaseUseShardForMiddleware):
    def process_request(self, request):
        # Return early if the request is not for /admin
        if not request.path.startswith('/admin/'):
            return None

        # _enable_shard_for() puts the cross-shard proxy on the request once it enters the shard of the override.
        _unwrap_the_cross_shard_proxy(request)

        if not _user_is_authenticated(request):
            # As in BaseAdminOverrideUseShardMiddleware.process_request().
            self.set_shard_context_manager(request, None)
            request._mapping_value = None
            _check_maintenance_status(request)
            return None

        if result := super().process_request(request):
            return result

        # Check maintenance status for both GET and POST requests
        is_maintenance = _check_maintenance_status(request, mapping_value=request._mapping_value)

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
        mapping_class = get_mapping_class()
        try:
            mapping = find_switchable_mapping(request, mapping_class, mapping_class.mapping_field, target_value)
        except PermissionDenied:
            request._mapping_value = None
            _refuse_an_override_that_is_not_switchable(
                request,
                target_value,
                'Cleared the override, because you do not have access to the selected mapping value.',
            )
            return
        if mapping is None:
            # As in BaseAdminOverrideUseShardMiddleware._enable_shard().
            request._mapping_value = None
            return
        request.user = CrossShardMappingUserProxy(
            request.user, quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_main_value(request)
        )
        # Allow access to maintenance shards for the admin (but read-only)
        self.set_shard_context_manager(
            request, use_shard(mapping.shard, mapping_value=target_value, active_only_schemas=False)
        ).enable()


class ShardIdAdminOverrideMiddleware(BaseAdminOverrideUseShardMiddleware):
    def get_shard_id(self, request):
        return request.session.get(quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.override_shard_selector_key, None)


class MappingValueAdminOverrideMiddleware(BaseAdminOverrideUseShardForMiddleware):
    def get_mapping_value(self, request):
        return request.session.get(quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.override_shard_selector_key, None)
