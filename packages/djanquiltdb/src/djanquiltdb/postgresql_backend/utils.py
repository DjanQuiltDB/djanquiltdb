import hashlib
from contextlib import contextmanager

from django.db.backends import utils


class LockCursorWrapperMixin:
    def __init__(self, *args, lock, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = lock

    @contextmanager
    def _lock(self):
        lock_keys = self.lock and self.db.shard_options.lock_keys

        if not lock_keys:
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
        cursor.acquire_advisory_lock_for_each_key(lock_keys, shared=True, xact=xact)

        try:
            yield
        finally:
            if not xact:
                cursor.release_advisory_lock_for_each_key(lock_keys, shared=True)

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
        return self.acquire_advisory_lock_for_each_key([key], shared=shared, xact=xact)

    def release_advisory_lock(self, key, shared=True):
        """
        Release a shared or exclusive advisory lock on a given key.
        """
        return self.release_advisory_lock_for_each_key([key], shared=shared)

    def acquire_advisory_lock_for_each_key(self, keys, shared=True, xact=False):
        """
        Sets a shared or exclusive advisory lock on each of ``keys`` in one statement, session-scoped by default or,
        with xact=True, scoped to the current transaction. Postgres evaluates the select list from left to right, so
        the locks are taken in the order of ``keys``.
        """
        function = 'pg_advisory{}_lock{}'.format('_xact' if xact else '', '_shared' if shared else '')
        return self._execute_function_for_each_key(function, keys)

    def release_advisory_lock_for_each_key(self, keys, shared=True):
        """
        Releases a shared or exclusive advisory lock on each of ``keys`` in one statement, in the order of ``keys``.
        """
        function = 'pg_advisory_unlock{}'.format('_shared' if shared else '')
        return self._execute_function_for_each_key(function, keys)

    def _execute_function_for_each_key(self, function, keys):
        """
        Runs one statement that calls ``function`` once for each of ``keys``, with the number that get_int_from_key()
        makes of the key. Without keys, no statement runs.
        """
        if not keys:
            return None

        return super().execute(
            'SELECT {};'.format(', '.join('{}(%s)'.format(function) for _ in keys)),
            [self.get_int_from_key(key) for key in keys],
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
