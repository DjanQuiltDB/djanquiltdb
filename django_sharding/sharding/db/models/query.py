import functools
import inspect
import types

from django.db.models import QuerySet as BaseQuerySet

from sharding.db import connection
from sharding.decorators import _add_decorator_reference, class_method_use_shard
from sharding.options import InstanceShardOptions


def post_init():
    def outer(func):
        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            init = func(self, *args, **kwargs)

            self._shard = InstanceShardOptions.from_connection(connection)

            return init

        return _add_decorator_reference(inner, decorator=post_init)

    return outer


class QuerySetMetaClass(type):
    def __new__(mcs, name, bases, attrs):
        new_class = super().__new__(mcs, name, bases, attrs)
        new_class.__init__ = post_init()(new_class.__init__)

        for attr, func in inspect.getmembers(new_class, inspect.isfunction):
            # getattr(model, attr) will trigger dynamic lookup via the descriptor protocol,  __getattr__ or
            # __getattribute__. Therefore, we use inspect.getattr_static to strip out staticmethods (which we don't want
            # to decorate). Furthermore, we also don't have to decorate the __init__, since it's already decorated with
            # the post_init decorator.
            if not isinstance(inspect.getattr_static(new_class, attr), types.FunctionType) or attr == '__init__':
                continue

            setattr(new_class, attr, class_method_use_shard()(func))

        return new_class


class QuerySet(BaseQuerySet, metaclass=QuerySetMetaClass):
    pass
