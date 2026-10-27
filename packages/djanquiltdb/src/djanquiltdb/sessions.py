import functools
import re

from django.conf import settings
from django.contrib.sessions.backends.base import VALID_KEY_CHARS
from django.contrib.sessions.backends.cached_db import SessionStore as BaseSessionStore
from django.core import signing
from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

from djanquiltdb.utils import NOT_ASYNC_SAFE, State, for_each_shard, shard_selector_pattern, use_shard, use_shard_for


@functools.cache
def _get_shard_selector_regex() -> re.Pattern:
    return shard_selector_pattern()


@functools.cache
def _get_delimiter() -> str:
    return str(getattr(settings, 'QUILT_SESSIONS', {}).get('SESSION_KEY_DELIMITER', 'K'))


@functools.cache
def _get_session_key_regex() -> re.Pattern:
    shard_selector = _get_shard_selector_regex()
    delimiter = _get_delimiter()

    if shard_selector.match(delimiter):
        raise ImproperlyConfigured(
            'DjanQuiltDB session delimiter {} is not mutually exclusive with SHARD_SELECTOR_REGEX {}'.format(
                delimiter, shard_selector
            )
        )

    # When taking the shard selector regex, we remove the anchors ^ and $
    return re.compile('^S(' + shard_selector.pattern[1:-1] + ')' + delimiter)


@receiver(setting_changed)
def reset_cached_session_settings(setting, **kwargs):
    """
    Clears the cached regexes and delimiter when override_settings() changes QUILT_DB or QUILT_SESSIONS.
    """
    if setting in ('QUILT_DB', 'QUILT_SESSIONS'):
        _get_shard_selector_regex.cache_clear()
        _get_delimiter.cache_clear()
        _get_session_key_regex.cache_clear()


class SessionStore(BaseSessionStore):
    salt = 'djanquiltdb.sessions'

    def __init__(self, session_key=None, shard_selector=None):
        super().__init__(session_key)
        self.__shard_selector = shard_selector

    @classmethod
    def get_model_class(cls):
        if 'SESSION_MODEL' not in getattr(settings, 'QUILT_SESSIONS', {}):
            raise ImproperlyConfigured('To use djanquiltdb.sessions.SessionStore, you must define SESSION_MODEL')

        model_class = import_string(settings.QUILT_SESSIONS['SESSION_MODEL'])

        from djanquiltdb.models import BaseQuiltSession

        if not issubclass(model_class, BaseQuiltSession):
            raise ImproperlyConfigured(f"SESSION_MODEL {model_class} doesn't subclass BaseQuiltSession")

        return model_class

    @property
    def shard_selector(self):
        if self.__shard_selector is not None:
            return self.__shard_selector

        if not self.session_key:
            return

        try:
            deserialized_key = signing.loads(
                self.session_key, serializer=self.serializer, max_age=settings.SESSION_COOKIE_AGE, salt=self.salt
            )
        except signing.BadSignature:
            # Expired or tampered session key; treat as anonymous and discard the key so SessionBase mints a fresh one
            # on next write. Matches Django's signed_cookies backend, which also returns an empty session here.
            self._session_key = None
            return None

        match = _get_session_key_regex().search(deserialized_key)
        if not match:
            # A key signed before SHARD_SELECTOR_REGEX or SESSION_KEY_DELIMITER changed holds no selector that the
            # backend can read; it is discarded like a key with a bad signature.
            self._session_key = None
            return None

        self.__shard_selector = match.group(1)
        return self.__shard_selector

    @shard_selector.setter
    def shard_selector(self, value):
        if not _get_shard_selector_regex().match(str(value)):
            raise ImproperlyConfigured(
                'DjanQuiltDB session backend tried to set selector {} which is unmatched by '
                'SHARD_SELECTOR_REGEX {}'.format(value, _get_shard_selector_regex().pattern)
            )
        if _get_shard_selector_regex().match(f'{value}{_get_delimiter()}'):
            raise ImproperlyConfigured(
                'DjanQuiltDB session SESSION_KEY_DELIMITER {} is ambiguous for selector {} with '
                'SHARD_SELECTOR_REGEX {}'.format(_get_delimiter(), value, _get_shard_selector_regex().pattern)
            )

        self.__shard_selector = value

    def _get_new_session_key(self):
        while True:
            session_key = get_random_string(32, VALID_KEY_CHARS)
            session_key = signing.dumps(
                f'S{self.shard_selector}{_get_delimiter()}{session_key}',
                compress=True,
                salt=self.salt,
                serializer=self.serializer,
            )
            if not self.exists(session_key):
                break
        return session_key

    def exists(self, session_key):
        with use_shard_for(self.shard_selector):
            return super().exists(session_key)

    def load(self):
        if self.shard_selector is None:
            # A key without a selector names no shard to read the session from, so the session is read as new and
            # empty, as for a selector that the mapping table no longer has.
            self._session_key = None
            return {}

        try:
            shard_context = use_shard_for(self.shard_selector)
        except ObjectDoesNotExist:
            # The mapping table has no shard for the selector, for example because the mapping was deleted. Like
            # Django's database backend with a key it does not find, the session is read as new and empty; the
            # selector goes too, so a later save cannot store the session under it.
            self._session_key = None
            self.__shard_selector = None
            return {}

        with shard_context:
            return super().load()

    def save(self, must_create=False):
        with use_shard_for(self.shard_selector):
            super().save(must_create)

    def delete(self, session_key=None):
        if self.shard_selector is None:
            # The session wasn't properly initialized
            return
        try:
            shard_context = use_shard_for(self.shard_selector)
        except ObjectDoesNotExist:
            # The mapping table has no shard for the selector, so there is no shard to delete the session from.
            return
        with shard_context:
            super().delete(session_key)

    # Django's async session methods read and write the database directly instead of through the sync ones above, so
    # they would run outside the shard of the session. These are the methods that every other async method reaches.

    async def aload(self):
        raise NotImplementedError('SessionStore.aload(): ' + NOT_ASYNC_SAFE)

    async def aexists(self, session_key):
        raise NotImplementedError('SessionStore.aexists(): ' + NOT_ASYNC_SAFE)

    async def acreate(self):
        raise NotImplementedError('SessionStore.acreate(): ' + NOT_ASYNC_SAFE)

    async def asave(self, must_create=False):
        raise NotImplementedError('SessionStore.asave(): ' + NOT_ASYNC_SAFE)

    async def adelete(self, session_key=None):
        raise NotImplementedError('SessionStore.adelete(): ' + NOT_ASYNC_SAFE)

    @classmethod
    async def aclear_expired(cls):
        raise NotImplementedError('SessionStore.aclear_expired(): ' + NOT_ASYNC_SAFE)

    @classmethod
    def clear_expired(cls):
        def _clear(shard):
            if shard.state != State.ACTIVE:
                # A shard being moved or purged is left alone; its expired sessions go on the next run.
                return
            with use_shard(shard):
                cls.get_model_class().objects.filter(expire_date__lt=timezone.now()).delete()

        for_each_shard(_clear)
