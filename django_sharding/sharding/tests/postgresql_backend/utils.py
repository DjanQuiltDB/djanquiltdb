from unittest import mock

from django.test import SimpleTestCase

from sharding.postgresql_backend.utils import LockCursorWrapperMixin, CursorDebugWrapper, CursorWrapper


class LockCursorWrapperTestCase(SimpleTestCase):
    class DummyCursorWrapper:
        def __init__(self, cursor, db):
            self.cursor = cursor
            self.db = db

        def execute(self, *args, **kwargs):
            return args, kwargs

        def executemany(self, *args, **kwargs):
            return args, kwargs

    class CursorWrapper(LockCursorWrapperMixin, DummyCursorWrapper):
        pass

    def setUp(self):
        super().setUp()

        self.cursor = self.cursor()
        self._db = self.cursor.db
        self._db.lock_on_execute = False

    def cursor(self):
        cursor = mock.Mock()
        db = mock.Mock()
        db.cursor.side_effect = lambda: self.CursorWrapper(cursor, db)
        return self.CursorWrapper(cursor, db)

    @mock.patch('hashlib.md5')
    def test_get_int_from_key(self, mock_md5):
        """
        Case: Call get_int_from_key
        Expected: hashlib.md5 to be called and the correct int to be returned
        """
        mock_md5.return_value = mock.Mock()
        mock_md5.return_value.update = mock.Mock()
        mock_md5.return_value.hexdigest = mock.Mock(return_value='098f6bcd4621d373cade4e832627b4f6')

        result = LockCursorWrapperMixin.get_int_from_key('test')

        mock_md5.assert_called_once_with()
        mock_md5.return_value.update.assert_called_once_with(b'test')
        self.assertEqual(result, 43055487337504055)

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', return_value='foo')
    def test_acquire_advisory_lock_shared(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call acquire_advisory_lock
        Expected: Cursor's execute called with pg_advisory_lock_shared and the key transformed with get_int_from_key
        """
        self.cursor.acquire_advisory_lock('bar')
        mock_get_int_from_key.assert_called_once_with('bar')
        mock_execute.assert_called_once_with('SELECT pg_advisory_lock_shared(%s);', ['foo'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', return_value='foo')
    def test_acquire_advisory_lock_exclusive(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call acquire_advisory_lock with shared=False
        Expected: Cursor's execute called with pg_advisory_lock and the key transformed with get_int_from_key
        """
        self.cursor.acquire_advisory_lock('bar', shared=False)
        mock_get_int_from_key.assert_called_once_with('bar')
        mock_execute.assert_called_once_with('SELECT pg_advisory_lock(%s);', ['foo'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', return_value='foo')
    def test_release_advisory_lock_shared(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call release_advisory_lock
        Expected: Cursor's execute called with pg_advisory_unlock_shared and the key transformed with get_int_from_key
        """
        self.cursor.release_advisory_lock('bar')
        mock_get_int_from_key.assert_called_once_with('bar')
        mock_execute.assert_called_once_with('SELECT pg_advisory_unlock_shared(%s);', ['foo'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', return_value='foo')
    def test_release_advisory_lock_exclusive(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call release_advisory_lock with shared=False
        Expected: Cursor's execute called with pg_advisory_unlock and the key transformed with get_int_from_key
        """
        self.cursor.release_advisory_lock('bar', shared=False)
        mock_get_int_from_key.assert_called_once_with('bar')
        mock_execute.assert_called_once_with('SELECT pg_advisory_unlock(%s);', ['foo'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    def test_no_lock_on_execute(self, mock_execute):
        """
        Case: Call the cursor's execute method while lock_on_execute is equal to an empty list
        Expected: Cursor's super() execute only called once with the SQL we provided
        """
        self.assertFalse(self._db.lock_on_execute)
        self.cursor.execute('SELECT * FROM dummy;')
        mock_execute.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db.cursor.called)  # No new cursor asked for acquiring lock

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', mock.MagicMock(return_value='bar'))
    def test_lock_on_execute(self, mock_execute):
        """
        Case: Call the cursor's execute method while lock_on_execute is not empty
        Expected: Cursor's super() execute three times called: one for acquiring an advisory lock, one for the SQL we
                  provided and one for releasing the advisory lock
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = ['foo']

        self.cursor.execute('SELECT * FROM dummy;')

        self.assertEqual(mock_execute.call_count, 3)

        mock_execute.assert_has_calls([
            mock.call('SELECT pg_advisory_lock_shared(%s);', ['bar']),
            mock.call('SELECT * FROM dummy;'),
            mock.call('SELECT pg_advisory_unlock_shared(%s);', ['bar']),
        ])

        self.assertEqual(self._db.cursor.call_count, 1)  # New cursor asked for acquiring and releasing locks

    @mock.patch.object(DummyCursorWrapper, 'executemany')
    def test_no_lock_on_execute_many(self, mock_executemany):
        """
        Case: Call the cursor's executemany method while lock_on_execute is equal to an empty list
        Expected: Cursor's super() executemany called once with the SQL we provided and no cursor's super() execute
                  called
        """
        self.assertFalse(self._db.lock_on_execute)
        self.cursor.executemany('SELECT * FROM dummy;')
        mock_executemany.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db.cursor.called)  # No new cursor asked for acquiring lock

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(DummyCursorWrapper, 'executemany')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', mock.MagicMock(return_value='bar'))
    def test_lock_on_execute_many(self, mock_executemany, mock_execute):
        """
        Case: Call the cursor's executemany method while lock_on_execute is not empty
        Expected: Cursor's super() execute two times called: one for acquiring an advisory lock and one for releasing
                  the advisory lock. The executemany method has been called once with the SQL we provided.
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = ['foo']

        self.cursor.executemany('SELECT * FROM dummy;')

        self.assertEqual(mock_execute.call_count, 2)

        mock_execute.assert_has_calls([
            mock.call('SELECT pg_advisory_lock_shared(%s);', ['bar']),
            mock.call('SELECT pg_advisory_unlock_shared(%s);', ['bar']),
        ])

        mock_executemany.assert_called_once_with('SELECT * FROM dummy;')

        self.assertEqual(self._db.cursor.call_count, 1)  # New cursor asked for acquiring and releasing locks


class CursorDebugWrapperTestCase(SimpleTestCase):
    def test_inherit_lock_cursor_wrapper_mixin(self):
        """
        Case: Check whether CursorDebugWrapper inherits LockCursorWrapperMixin
        Expected: CursorDebugWrapper inherits LockCursorWrapperMixin
        """
        self.assertTrue(issubclass(CursorDebugWrapper, LockCursorWrapperMixin))


class CursorWrapperTestCase(SimpleTestCase):
    def test_inherit_lock_cursor_wrapper_mixin(self):
        """
        Case: Check whether CursorWrapper inherits LockCursorWrapperMixin
        Expected: CursorWrapper inherits LockCursorWrapperMixin
        """
        self.assertTrue(issubclass(CursorWrapper, LockCursorWrapperMixin))
