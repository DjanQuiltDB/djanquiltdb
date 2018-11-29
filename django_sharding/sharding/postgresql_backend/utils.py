import hashlib
from contextlib import contextmanager

from django.db.backends import utils


class LockCursorWrapperMixin:
    @contextmanager
    def _lock(self):
        if not getattr(self.db, 'lock_on_execute', False):
            # No need to set an advisory lock on executing SQL queries, so we return early.
            yield
            return

        # Need to ask for a new cursor. Not doing that can cause methods like fetchall() to return results of our
        # locking queries instead of the query we actually want to perform.
        cursor = self.db.cursor()

        for key in self.db.shard_options.lock_keys:
            cursor.acquire_advisory_lock(key, shared=True)

        try:
            yield
        finally:
            for key in self.db.shard_options.lock_keys:
                cursor.release_advisory_lock(key, shared=True)

    def execute(self, *args, **kwargs):
        with self._lock():
            return super().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        with self._lock():
            return super().executemany(*args, **kwargs)

    def acquire_advisory_lock(self, key, shared=True):
        """
        Set a shared or exclusive advisory lock on a given key.
        """
        return super().execute('SELECT pg_advisory_lock{}(%s);'.format('_shared' if shared else ''),
                               [self.get_int_from_key(key)])

    def release_advisory_lock(self, key, shared=True):
        """
        Release a shared or exclusive advisory lock on a given key.
        """
        return super().execute('SELECT pg_advisory_unlock{}(%s);'.format('_shared' if shared else ''),
                               [self.get_int_from_key(key)])

    @staticmethod
    def get_int_from_key(key):
        """
        Turn the given id to a md5 hash and make an int from the first 60 bits of the hash
        """
        m = hashlib.md5()  # nosec
        m.update(key.encode())
        return int(m.hexdigest()[:15], 16)


class CursorDebugWrapper(LockCursorWrapperMixin, utils.CursorDebugWrapper):
    pass


class CursorWrapper(LockCursorWrapperMixin, utils.CursorWrapper):
    pass
