import hashlib

from django.db.backends import utils


class LockCursorWrapperMixin:
    def __init__(self, cursor, db):
        super().__init__(cursor, db)

        # Contains the lock keys that needs to be set on each cursor execute command. Will be an empty list if
        self.lock_on_execute = getattr(db, 'lock_on_execute', [])

    def execute(self, *args, **kwargs):
        for key in self.lock_on_execute:
            self.acquire_advisory_lock(key)

        try:
            return super().execute(*args, **kwargs)
        finally:
            for key in self.lock_on_execute:
                self.release_advisory_lock(key)

    def executemany(self, *args, **kwargs):
        for key in self.lock_on_execute:
            self.acquire_advisory_lock(key)

        try:
            return super().executemany(*args, **kwargs)
        finally:
            for key in self.lock_on_execute:
                self.release_advisory_lock(key)

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
