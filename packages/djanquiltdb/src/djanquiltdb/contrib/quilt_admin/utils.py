import contextlib
import functools

from django.core.exceptions import ImproperlyConfigured, PermissionDenied

from djanquiltdb.contrib.quilt_admin import apps as quilt_admin_apps
from djanquiltdb.models import BaseShard
from djanquiltdb.utils import get_shard_class, get_shard_for, public_schema_alias_for_reading, use_shard

"""
Proxy objects that ensure that even if the current request is on a different shard than where the user is stored,
it can still be used for lookups of e.g. name and permissions. This is needed to make the Quilt Admin render
contents for a selected shard while applying permissions based on the user as defined in its own shard.

These proxies are per-request and read-oriented. The Django admin reads request.user hundreds of times while
building its navigation, so re-resolving the user's home shard and re-acquiring an advisory lock on every single
attribute access is prohibitively expensive. To avoid that, each proxy resolves its home shard only once and
memoizes the result of every attribute lookup for its (request) lifetime.
"""


class _BaseCrossShardUserProxy:
    # Attributes that belong to the proxy itself and must never be routed through the shard-switching __getattr__.
    _OWN_ATTRS = frozenset({'_user', '_selector', '_home_shard', '_attr_cache'})

    def __init__(self, user, selector):
        # Set the instance attributes before the first call to __getattr__, so that __getattr__ does not recurse.
        self._user = user
        self._selector = selector  # a Shard or a shard id (id mode), or a mapping value (mapping mode)
        self._home_shard = None
        self._attr_cache = {}

    def _enter_home_shard(self):
        """Return a context manager that activates the user's home shard, without issuing a query per call."""
        raise NotImplementedError

    def __getattr__(self, name):
        # Dunder/protocol attributes (e.g. __deepcopy__, __reduce__, __class__ fallbacks) are delegated straight
        # to the wrapped user, preserving copy/pickle/hasattr/repr semantics without activating a shard.
        if name.startswith('__') and name.endswith('__'):
            return getattr(self._user, name)

        # If __getattr__ fires for one of our own attributes, it genuinely does not exist (it was not set yet).
        if name in self._OWN_ATTRS:
            raise AttributeError(name)

        cache = self._attr_cache
        if name in cache:
            return cache[name]

        # Resolve the attribute with the home shard active so lazy fields and related lookups route correctly.
        with self._enter_home_shard():
            value = getattr(self._user, name)

        # Only successful lookups are cached, so a missing attribute keeps raising AttributeError (hasattr stays
        # correct) instead of being masked by a cached value.
        cache[name] = value
        return value


class CrossShardUserProxy(_BaseCrossShardUserProxy):
    """
    Shard-id mode: the selector is the id of the home shard, as the session stores it. The proxy looks up the Shard once
    and reuses it. A selector that is a Shard object is used unchanged.
    """

    def _enter_home_shard(self):
        if self._home_shard is None:
            if isinstance(self._selector, BaseShard):
                self._home_shard = self._selector
            else:
                shard_class = get_shard_class()
                self._home_shard = shard_class.objects.using(public_schema_alias_for_reading(shard_class)).get(
                    id=self._selector
                )
        return use_shard(self._home_shard)


class CrossShardMappingUserProxy(_BaseCrossShardUserProxy):
    """Mapping mode: the selector is a mapping value, resolved to a Shard once and then reused."""

    def _enter_home_shard(self):
        if self._home_shard is None:
            self._home_shard = get_shard_for(self._selector)
        # Pass mapping_value through so the advisory lock keys match the previous use_shard_for behaviour.
        return use_shard(self._home_shard, mapping_value=self._selector)


def route_admin_log_to_home_shard(func):
    """
    Decorator for Django's ``ModelAdmin.log_*`` methods. While the admin is switched to another shard,
    ``request.user`` is wrapped in a cross-shard proxy and the active connection points at the viewed shard.
    Django's admin logging then tries to write a ``LogEntry`` into that shard's ``django_admin_log``, whose
    ``user_id`` foreign key references that shard's user table - where the admin user does not exist - so the
    write fails at COMMIT. When the user is a cross-shard proxy, run the wrapped method inside the user's home
    shard so the ``LogEntry`` (and its valid ``user_id`` FK) lands there instead. For a regular user this is a
    no-op.
    """

    @functools.wraps(func)
    def wrapper(self, request, *args, **kwargs):
        user = getattr(request, 'user', None)
        if isinstance(user, _BaseCrossShardUserProxy):
            with user._enter_home_shard():
                return func(self, request, *args, **kwargs)
        return func(self, request, *args, **kwargs)

    return wrapper


@contextlib.contextmanager
def _as_the_user_in_the_home_shard(request):
    """
    Puts the logged-in user on ``request`` in place of the cross-shard proxy, and enters the user's own shard, so that
    switchable_mappings() and switchable_shards() of the shard selector class get the same user and the same active
    shard in the override middleware, the shard switcher and the switch view.
    """
    proxy = getattr(request, 'user', None)
    if not isinstance(proxy, _BaseCrossShardUserProxy):
        yield
        return
    request.user = proxy._user
    try:
        with proxy._enter_home_shard():
            yield
    finally:
        request.user = proxy


def _call_switchable_hook(hook_name, request, queryset):
    """
    Returns the queryset that the hook ``hook_name`` of the shard selector class returns for ``queryset``. Raises
    ImproperlyConfigured when that queryset has a slice, because the callers filter and order it.
    """
    switchable = getattr(quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS, hook_name)(request, queryset)
    if switchable.query.is_sliced:
        raise ImproperlyConfigured(
            '{}() of {} returns a queryset with a slice. Return a queryset without a slice, because the shard switcher '
            'and the override middleware filter and order it.'.format(
                hook_name, quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.__name__
            )
        )
    return switchable


def _queryset_of_all_mappings(mapping_class):
    return mapping_class.objects.using(public_schema_alias_for_reading(mapping_class)).select_related('shard').all()


def _queryset_of_all_shards(shard_class):
    return shard_class.objects.using(public_schema_alias_for_reading(shard_class)).all()


def list_switchable_mappings(request, mapping_class):
    """
    Returns a list of the mappings that switchable_mappings() of the shard selector class returns, in the order of
    their primary keys.
    """
    with _as_the_user_in_the_home_shard(request):
        mappings = _call_switchable_hook('switchable_mappings', request, _queryset_of_all_mappings(mapping_class))
        return list(mappings.order_by('pk'))


def list_switchable_shards(request, shard_class):
    """
    Returns a list of the shards that switchable_shards() of the shard selector class returns, in the order of their
    primary keys.
    """
    with _as_the_user_in_the_home_shard(request):
        shards = _call_switchable_hook('switchable_shards', request, _queryset_of_all_shards(shard_class))
        return list(shards.order_by('pk'))


def find_switchable_mapping(request, mapping_class, mapping_field, mapping_value):
    """
    Returns the mapping whose ``mapping_field`` is ``mapping_value``, or None when the mapping table has no such
    mapping. Raises PermissionDenied when the mapping exists but switchable_mappings() of the shard selector class does
    not return it.
    """
    with _as_the_user_in_the_home_shard(request):
        all_mappings = _queryset_of_all_mappings(mapping_class)
        # The value is filtered on the queryset that the hook returns, so that the result is the requested mapping
        # also when the hook ignores the queryset that it receives.
        mapping = (
            _call_switchable_hook('switchable_mappings', request, all_mappings)
            .filter(**{mapping_field: mapping_value})
            .first()
        )
        if mapping is None and all_mappings.filter(**{mapping_field: mapping_value}).exists():
            raise PermissionDenied
    return mapping


def find_switchable_shard(request, shard_id):
    """
    Returns the shard with the id ``shard_id``, or None when the shard registry has no such shard. Raises
    PermissionDenied when the shard exists but switchable_shards() of the shard selector class does not return it.
    """
    with _as_the_user_in_the_home_shard(request):
        all_shards = _queryset_of_all_shards(get_shard_class())
        # As in find_switchable_mapping().
        shard = _call_switchable_hook('switchable_shards', request, all_shards).filter(id=shard_id).first()
        if shard is None and all_shards.filter(id=shard_id).exists():
            raise PermissionDenied
    return shard
