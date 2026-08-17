import functools

from djanquiltdb.utils import get_shard_for, use_shard

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
        # Set real instance attributes before any __getattr__ can fire, to avoid recursion.
        self._user = user
        self._selector = selector  # a Shard object (id mode) or a mapping value (mapping mode)
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
    """Shard-id mode: the selector is an already-resolved Shard object."""

    def _enter_home_shard(self):
        return use_shard(self._selector)


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
