import hashlib
from contextlib import contextmanager

from django.db.backends import utils


class LockCursorWrapperMixin:
    def __init__(self, *args, lock, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = lock

    @contextmanager
    def _lock(self):
        if not self.lock or not self.db.shard_options.lock_keys:
            # No need to set an advisory lock on executing SQL queries, so we return early.
            yield
            return

        # Need to ask for a new cursor. Not doing that can cause methods like fetchall() to return results of our
        # locking queries instead of the query we actually want to perform.
        cursor = self.db._get_cursor(skip_lock=True)

        # Inside a transaction the locks are taken transaction-scoped: a failing statement aborts the transaction,
        # which would make the unlock below fail as well, masking the statement's own error and stranding the
        # session-level lock until the connection closes. A xact lock travels with the transaction instead and is
        # released by its commit or rollback.
        xact = self.db.in_atomic_block
        for key in self.db.shard_options.lock_keys:
            cursor.acquire_advisory_lock(key, shared=True, xact=xact)

        try:
            yield
        finally:
            if not xact:
                for key in self.db.shard_options.lock_keys:
                    cursor.release_advisory_lock(key, shared=True)

            cursor.close()

    def execute(self, *args, **kwargs):
        with self._lock():
            return super().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        with self._lock():
            return super().executemany(*args, **kwargs)

    def acquire_advisory_lock(self, key, shared=True, xact=False):
        """
        Set a shared or exclusive advisory lock on a given key, session-scoped by default or, with xact=True,
        scoped to the current transaction so its commit or rollback releases the lock.
        """
        return super().execute(
            'SELECT pg_advisory{}_lock{}(%s);'.format('_xact' if xact else '', '_shared' if shared else ''),
            [self.get_int_from_key(key)],
        )

    def release_advisory_lock(self, key, shared=True):
        """
        Release a shared or exclusive advisory lock on a given key.
        """
        return super().execute(
            'SELECT pg_advisory_unlock{}(%s);'.format('_shared' if shared else ''), [self.get_int_from_key(key)]
        )

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
