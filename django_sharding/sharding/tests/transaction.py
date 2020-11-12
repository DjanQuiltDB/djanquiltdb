from unittest import mock

from django.test import SimpleTestCase, override_settings

from sharding.db import connection
from sharding.transaction import atomic
from sharding.utils import use_shard


@mock.patch('sharding.transaction.Atomic')
@mock.patch('sharding.transaction.transaction_for_nodes')
class AtomicTestCase(SimpleTestCase):
    def test_default_node(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic from a default connection, and the default is also the primary
        Expected: transaction_for_nodes not called. Just the normal Atomic
        """
        with use_shard(node_name='default', schema_name='public'):
            atomic(using=None, savepoint=True)

        self.assertFalse(mock_transaction_for_nodes.called)
        mock_atomic.assert_called_once_with(None, True)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_primary_node_sharded(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic from a sharded context that is not the same as the primary
        Expected: transaction_for_nodes called for both the primary and the current connection
        """
        with use_shard(node_name='default', schema_name='public') as env:
            atomic(using=None, savepoint=True)

            mock_transaction_for_nodes.assert_called_once_with(nodes=['other', env.connection])
            self.assertFalse(mock_atomic.called)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_primary_node(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic from the default connection, which is not the same as the primary
        Expected: transaction_for_nodes called for both the primary and the current connection
        """
        atomic(using=None, savepoint=True)

        mock_transaction_for_nodes.assert_called_once_with(nodes=['other', connection])
        self.assertFalse(mock_atomic.called)

    def test_non_primary_node_sharded(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic from a non-primary connection from a sharded context
        Expected: transaction_for_nodes called for both the primary and the current connection
        """
        with use_shard(node_name='other', schema_name='public') as env:
            atomic(using=None, savepoint=True)

            mock_transaction_for_nodes.assert_called_once_with(nodes=['default', env.connection])
            self.assertFalse(mock_atomic.called)

    def test_using_sharded(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic for a non-primary connection sharded context, and provide a `using` argument
        Expected: transaction_for_nodes not called. Just the normal Atomic.
        """
        with use_shard(node_name='other', schema_name='public'):
            atomic(using='other', savepoint=True)

        self.assertFalse(mock_transaction_for_nodes.called)
        mock_atomic.assert_called_once_with('other', True)

    def test_using(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic for a non-primary connection, and provide a `using` argument
        Expected: transaction_for_nodes not called. Just the normal Atomic.
        """
        atomic(using='other', savepoint=True)

        self.assertFalse(mock_transaction_for_nodes.called)
        mock_atomic.assert_called_once_with('other', True)

    def test_callable(self, mock_transaction_for_nodes, mock_atomic):
        """
        Case: Call atomic with a function as using argument
        Expected: atomic calls itself with using=None, and normal behavior resumes
        """
        def some_function():
            pass

        with self.subTest("From a sharded context"):
            mock_transaction_for_nodes.reset_mock()
            mock_atomic.reset_mock()

            with use_shard(node_name='other', schema_name='public') as env:
                atomic(some_function)

                mock_transaction_for_nodes.assert_called_once_with(nodes=['default', env.connection])
                self.assertFalse(mock_atomic.called)

        with self.subTest("Not in a sharded context"):
            mock_transaction_for_nodes.reset_mock()
            mock_atomic.reset_mock()

            atomic(some_function)

            self.assertFalse(mock_transaction_for_nodes.called)
            mock_atomic.assert_called_once_with(None, True)
