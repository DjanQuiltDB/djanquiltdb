from unittest import mock

from django.test import SimpleTestCase

from djanquiltdb.postgresql_backend.utils import CursorDebugWrapper, CursorWrapper, LockCursorWrapperMixin


class LockCursorWrapperTestCase(SimpleTestCase):
    class DummyCursorWrapper:
        def __init__(self, cursor, db):
            self.cursor = cursor
            self.db = db

        def execute(self, *args, **kwargs):
            return args, kwargs

        def executemany(self, *args, **kwargs):
            return args, kwargs

        def close(self):
            pass

    class CursorWrapper(LockCursorWrapperMixin, DummyCursorWrapper):
        pass

    def setUp(self):
        super().setUp()

        self._db = self._get_db()
        self.cursor = self._db._get_cursor()
        self._db._get_cursor.reset_mock()  # Reset the calls to _get_cursor during the setUp

    def _get_db(self):
        db = mock.Mock()
        db.lock_on_execute = False
        db.in_atomic_block = False
        db.shard_options.lock_keys = []
        db._get_cursor.side_effect = self._get_cursor(db)
        return db

    def _get_cursor(self, db):
        def inner(name=None, skip_lock=False):
            cursor = mock.Mock()
            return self.CursorWrapper(cursor, db, lock=getattr(db, 'lock_on_execute', False) and not skip_lock)

        return inner

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
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', side_effect=lambda key: '{}_int'.format(key))
    def test_acquire_advisory_lock_for_each_key(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call acquire_advisory_lock_for_each_key with two keys, for shared and for exclusive locks, scoped to the
              session and to the transaction.
        Expected: Cursor's execute called once, with one lock function for each key, in the order of the keys.
        """
        statements = {
            (True, False): 'SELECT pg_advisory_lock_shared(%s), pg_advisory_lock_shared(%s);',
            (False, False): 'SELECT pg_advisory_lock(%s), pg_advisory_lock(%s);',
            (True, True): 'SELECT pg_advisory_xact_lock_shared(%s), pg_advisory_xact_lock_shared(%s);',
            (False, True): 'SELECT pg_advisory_xact_lock(%s), pg_advisory_xact_lock(%s);',
        }

        for (shared, xact), statement in statements.items():
            with self.subTest(shared=shared, xact=xact):
                mock_execute.reset_mock()

                self.cursor.acquire_advisory_lock_for_each_key(['first', 'second'], shared=shared, xact=xact)

                mock_execute.assert_called_once_with(statement, ['first_int', 'second_int'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', side_effect=lambda key: '{}_int'.format(key))
    def test_release_advisory_lock_for_each_key(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call release_advisory_lock_for_each_key with two keys, for shared and for exclusive locks.
        Expected: Cursor's execute called once, with one unlock function for each key, in the order of the keys.
        """
        statements = {
            True: 'SELECT pg_advisory_unlock_shared(%s), pg_advisory_unlock_shared(%s);',
            False: 'SELECT pg_advisory_unlock(%s), pg_advisory_unlock(%s);',
        }

        for shared, statement in statements.items():
            with self.subTest(shared=shared):
                mock_execute.reset_mock()

                self.cursor.release_advisory_lock_for_each_key(['first', 'second'], shared=shared)

                mock_execute.assert_called_once_with(statement, ['first_int', 'second_int'])

    @mock.patch.object(DummyCursorWrapper, 'execute')
    def test_no_statement_runs_for_an_empty_list_of_lock_keys(self, mock_execute):
        """
        Case: Call acquire_advisory_lock_for_each_key and release_advisory_lock_for_each_key without keys.
        Expected: No statement runs, because a statement without lock functions is not valid SQL.
        """
        self.cursor.acquire_advisory_lock_for_each_key([])
        self.cursor.release_advisory_lock_for_each_key([])

        mock_execute.assert_not_called()

    @mock.patch.object(DummyCursorWrapper, 'execute')
    def test_no_lock_on_execute(self, mock_execute):
        """
        Case: Call the cursor's execute method while lock_on_execute is equal to False
        Expected: Cursor's super() execute only called once with the SQL we provided
        """
        self.assertFalse(self._db.lock_on_execute)
        self.cursor.execute('SELECT * FROM dummy;')
        mock_execute.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db._get_cursor.called)  # No new cursor asked for acquiring lock

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
        self.cursor = self._db._get_cursor()

        # Reset the _get_cursor mock counter, because it is already called multiple times at this point during the
        # setup above.
        self._db._get_cursor.reset_mock()

        self.cursor.execute('SELECT * FROM dummy;')

        self.assertEqual(mock_execute.call_count, 3)

        mock_execute.assert_has_calls(
            [
                mock.call('SELECT pg_advisory_lock_shared(%s);', ['bar']),
                mock.call('SELECT * FROM dummy;'),
                mock.call('SELECT pg_advisory_unlock_shared(%s);', ['bar']),
            ]
        )

        # New cursor asked for acquiring and releasing locks
        self._db._get_cursor.assert_called_once_with(skip_lock=True)

    @mock.patch.object(DummyCursorWrapper, 'execute')
    @mock.patch.object(LockCursorWrapperMixin, 'get_int_from_key', side_effect=lambda key: '{}_int'.format(key))
    def test_lock_on_execute_with_two_lock_keys(self, mock_get_int_from_key, mock_execute):
        """
        Case: Call the cursor's execute method while lock_on_execute is True and the connection's shard_options have
              two lock keys.
        Expected: Cursor's super() execute called three times: one statement that locks both keys, the SQL we provided
                  and one statement that unlocks both keys.
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = ['shard_1', 'mapping_42']
        self.cursor = self._db._get_cursor()

        self.cursor.execute('SELECT * FROM dummy;')

        self.assertEqual(
            mock_execute.call_args_list,
            [
                mock.call(
                    'SELECT pg_advisory_lock_shared(%s), pg_advisory_lock_shared(%s);',
                    ['shard_1_int', 'mapping_42_int'],
                ),
                mock.call('SELECT * FROM dummy;'),
                mock.call(
                    'SELECT pg_advisory_unlock_shared(%s), pg_advisory_unlock_shared(%s);',
                    ['shard_1_int', 'mapping_42_int'],
                ),
            ],
        )

    @mock.patch.object(DummyCursorWrapper, 'execute')
    def test_lock_on_execute_no_lock_keys(self, mock_execute):
        """
        Case: Call the cursor's execute method while lock_on_execute is equal to True, but there are no lock keys set
              on the connection's shard_options
        Expected: Cursor's super() execute only called once with the SQL we provided
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = []
        self.cursor = self._db._get_cursor()

        # Reset the _get_cursor mock counter, because it is already called multiple times at this point during the
        # setup above.
        self._db._get_cursor.reset_mock()

        self.cursor.execute('SELECT * FROM dummy;')
        mock_execute.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db._get_cursor.called)  # No new cursor asked for acquiring lock

    @mock.patch.object(DummyCursorWrapper, 'executemany')
    def test_no_lock_on_execute_many(self, mock_executemany):
        """
        Case: Call the cursor's executemany method while lock_on_execute is equal to False
        Expected: Cursor's super() executemany called once with the SQL we provided and no cursor's super() execute
                  called
        """
        self.assertFalse(self._db.lock_on_execute)
        self.cursor.executemany('SELECT * FROM dummy;')
        mock_executemany.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db._get_cursor.called)  # No new cursor asked for acquiring lock

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
        self.cursor = self._db._get_cursor()

        # Reset the _get_cursor mock counter, because it is already called multiple times at this point during the
        # setup above.
        self._db._get_cursor.reset_mock()

        self.cursor.executemany('SELECT * FROM dummy;')

        self.assertEqual(mock_execute.call_count, 2)

        mock_execute.assert_has_calls(
            [
                mock.call('SELECT pg_advisory_lock_shared(%s);', ['bar']),
                mock.call('SELECT pg_advisory_unlock_shared(%s);', ['bar']),
            ]
        )

        mock_executemany.assert_called_once_with('SELECT * FROM dummy;')

        # New cursor asked for acquiring and releasing locks
        self._db._get_cursor.assert_called_once_with(skip_lock=True)

    @mock.patch.object(DummyCursorWrapper, 'executemany')
    def test_lock_on_execute_no_lock_keys_many(self, mock_executemany):
        """
        Case: Call the cursor's executemany method while lock_on_execute is equal to True, but there are no lock keys
              set on the connection's shard_options
        Expected: Cursor's super() executemany called once with the SQL we provided and no cursor's super() execute
                  called
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = []
        self.cursor = self._db._get_cursor()

        # Reset the _get_cursor mock counter, because it is already called multiple times at this point during the
        # setup above.
        self._db._get_cursor.reset_mock()

        self.cursor.executemany('SELECT * FROM dummy;')
        mock_executemany.assert_called_once_with('SELECT * FROM dummy;')
        self.assertFalse(self._db._get_cursor.called)  # No new cursor asked for acquiring lock

    @mock.patch.object(DummyCursorWrapper, 'close')
    @mock.patch.object(LockCursorWrapperMixin, 'acquire_advisory_lock_for_each_key')
    @mock.patch.object(LockCursorWrapperMixin, 'release_advisory_lock_for_each_key')
    def test_lock_cursor(self, mock_release_advisory_lock, mock_acquire_advisory_lock, mock_close):
        """
        Case: Use LockCursorWrapperMixin._lock() as context manager
        Expected: When inside the contextmanager, a new cursor has been asked, the advisory locks are set, but the locks
                  are not released yet and the cursor for the locking is not closed. After the context manager, the
                  locks are released and the cursor is closed.
        """
        self._db.lock_on_execute = True
        self._db.shard_options.lock_keys = ['foo']
        self.cursor = self._db._get_cursor()

        # Reset the _get_cursor mock counter, because it is already called multiple times at this point during the
        # setup above.
        self._db._get_cursor.reset_mock()

        with self.cursor._lock():
            self._db._get_cursor.assert_called_once_with(skip_lock=True)
            mock_acquire_advisory_lock.assert_called_once_with(['foo'], shared=True, xact=False)
            self.assertFalse(mock_release_advisory_lock.called)
            self.assertFalse(mock_close.called)

        mock_release_advisory_lock.assert_called_once_with(['foo'], shared=True)
        mock_close.assert_called_once_with()


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
