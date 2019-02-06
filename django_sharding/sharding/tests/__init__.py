import inspect
import itertools

from django.apps import apps
from django.db import connections, DEFAULT_DB_ALIAS
from django.test import TestCase, TransactionTestCase

from sharding.router import set_active_connection
from sharding.utils import ShardingMode, get_model_sharding_mode


class CleanShardingArtifactsMixin:
    def _fixture_setup(self):
        """
        Save the names of the schemas that exist on start of the test case.
        """
        self._initial_schemas = {}
        for db_name in self._databases_names():
            connection_ = connections[db_name]
            self._initial_schemas[db_name] = {s[0] for s in connection_.get_all_pg_schemas()}

        super()._fixture_setup()

    def _post_teardown(self):
        """
        Remove all the schemas that exist now, but didn't at the start of the test case.
        """
        super()._post_teardown()

        for db_name in self._databases_names():
            connection_ = connections[db_name]

            for schema in itertools.chain.from_iterable(connection_.get_all_pg_schemas()):
                if schema not in self._initial_schemas[db_name]:
                    connection_.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))


class ResetConnectionTestCaseMixin:
    """
    Makes sure that at the end of each test (and as fallback, at the beginning of each test) the connection is set to
    public.
    """
    def _pre_setup(self):
        self._reset_connections_to_public()
        super()._pre_setup()

    def _post_teardown(self):
        self._reset_connections_to_public()
        super()._post_teardown()

    def _reset_connections_to_public(self):
        set_active_connection(DEFAULT_DB_ALIAS)


class ShardingTestCase(ResetConnectionTestCaseMixin, TestCase):
    available_apps = ['sharding', 'example']
    multi_db = True  # To make sure cleanup will be done on all databases


class ShardingTransactionTestCase(ResetConnectionTestCaseMixin, CleanShardingArtifactsMixin, TransactionTestCase):
    available_apps = ['sharding', 'example']
    multi_db = True  # To make sure cleanup will be done on all databases

    @staticmethod
    def get_all_non_sharded_models():
        models = apps.get_models()
        return [model for model in models if get_model_sharding_mode(model) != ShardingMode.SHARDED
                and not getattr(model, 'test_model', False)]

    @staticmethod
    def get_all_mirrored_models():
        models = apps.get_models()
        return [model for model in models if get_model_sharding_mode(model) == ShardingMode.MIRRORED
                and not getattr(model, 'test_model', False)]


class DecoratorTestCaseMixin:
    def get_decorators_recursive(self, func):
        # `functools.wraps` adds a `__wrapped__` attribute that indicates
        # the function is decorated.
        if hasattr(func, '__wrapped__'):
            # Use the `__decorator__` attribute to identify the decorator.
            if hasattr(func, '__decorator__'):
                yield func.__decorator__

            yield from self.get_decorators_recursive(func.__wrapped__)

    def assertDecoratedWith(self, func, decorator):
        """
        Tests whether the given method is decorated with the given decorator.
        """
        decorators = list(self.get_decorators_recursive(func))
        self.assertIn(decorator, [fn for fn, _ in decorators])

    def assertDecoratorCalledWith(self, func, decorator, *args, **kwargs):
        """
        Tests whether the given method is decorated with the given decorator
        and called with the given args and kwargs.
        """
        self.assertDecoratedWith(func, decorator)

        # Get the decorator we are looking for
        for decorator_func, decorator_bound_arguments in self.get_decorators_recursive(func):
            if decorator_func == decorator:
                break

        bound_arguments = inspect.signature(decorator).bind(*args, **kwargs)

        self.assertEqual(bound_arguments.args, decorator_bound_arguments.args)
        self.assertEqual(bound_arguments.kwargs, decorator_bound_arguments.kwargs)
