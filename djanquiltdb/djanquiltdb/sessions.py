import functools
import re

from django.conf import settings
from django.contrib.sessions.backends.base import VALID_KEY_CHARS
from django.contrib.sessions.backends.cached_db import SessionStore as BaseSessionStore
from django.core import signing
from django.core.exceptions import ImproperlyConfigured
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

from djanquiltdb.utils import use_shard_for


@functools.cache
def _get_shard_selector_regex() -> re.Pattern:
    return re.compile('^' + getattr(settings, 'QUILT_SESSIONS', {}).get('SHARD_SELECTOR_REGEX', '[0-9]+') + '$')


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
            # Expired or tampered session key — treat as anonymous and discard the
            # key so SessionBase mints a fresh one on next write. Matches Django's
            # signed_cookies backend, which also returns an empty session here.
            self._session_key = None
            return None

        match = _get_session_key_regex().search(deserialized_key)
        self.__shard_selector = match.group(1) if match else None
        return self.__shard_selector

    @shard_selector.setter
    def shard_selector(self, value):
        if not _get_shard_selector_regex().match(str(value)):
            raise ImproperlyConfigured(
                'DjanQuiltDB session backend tried to set selector {} which is unmatched by SHARD_SELECTOR_REGEX {}'.format(
                    value, _get_shard_selector_regex().pattern
                )
            )
        if _get_shard_selector_regex().match(f'{value}{_get_delimiter()}'):
            raise ImproperlyConfigured(
                'DjanQuiltDB session SESSION_KEY_DELIMITER {} is ambiguous for selector {} with SHARD_SELECTOR_REGEX {}'.format(
                    _get_delimiter(), value, _get_shard_selector_regex().pattern
                )
            )

        self.__shard_selector = value

    def _get_new_session_key(self):
        while True:
            session_key = get_random_string(32, VALID_KEY_CHARS)
            session_key = signing.dumps(
                f'S{self.shard_selector}K{session_key}',
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
        with use_shard_for(self.shard_selector):
            return super().load()

    def save(self, must_create=False):
        with use_shard_for(self.shard_selector):
            super().save(must_create)

    def delete(self, session_key=None):
        if self.shard_selector is None:
            # The session wasn't properly initialized
            return
        with use_shard_for(self.shard_selector):
            super().delete(session_key)

    @classmethod
    def clear_expired(cls):
        raise NotImplementedError("It's not trivial to clear expired sessions throughout the all shards")
