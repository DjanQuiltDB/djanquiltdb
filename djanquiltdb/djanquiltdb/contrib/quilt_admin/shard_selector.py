from abc import ABC, abstractmethod
from typing import Any, Optional

from django.http import HttpRequest
from django.utils.functional import classproperty

from djanquiltdb import State
from djanquiltdb.models import BaseShard


class BaseAdminShardSelector(ABC):
    """
    Base class for one to be configured under settings.QUILT_ADMIN['SHARD_SELECTOR_CLASS']. Default is
    AdminShardSelector. This helps the shard selector logic in the Quilt Admin to set/get the override shard ID in the
    correct location, and format the entry in the shard selector dropdown.
    """

    @classproperty
    def override_shard_selector_key(self) -> str:
        from django.conf import settings

        if hasattr(settings, 'QUILT_ADMIN') and 'OVERRIDE_SHARD_SELECTOR_KEY' in settings.QUILT_ADMIN:
            return settings.QUILT_ADMIN['OVERRIDE_SHARD_SELECTOR_KEY']
        return 'admin_override_shard_selector'

    @classmethod
    @abstractmethod
    def retrieve_override_value(cls, request: HttpRequest) -> Optional[Any]:
        pass

    @classmethod
    @abstractmethod
    def set_override_value(cls, request: HttpRequest, value: Any | None) -> None:
        pass

    @classmethod
    @abstractmethod
    def format_override_label(cls, value: Any, shard: BaseShard | None, mapping: Any | None = None) -> str:
        pass

    @classmethod
    def format_override_option(cls, value: Any, shard: BaseShard | None, mapping: Any | None = None) -> str:
        if (mapping and mapping.state == State.MAINTENANCE) or (shard and shard.state == State.MAINTENANCE):
            maintenance_flag = ' [Maintenance]'
        else:
            maintenance_flag = ''

        return f'{cls.format_override_label(value=value, shard=shard, mapping=mapping)}{maintenance_flag}'

    @classmethod
    def retrieve_main_value(cls, request: HttpRequest) -> Optional[Any]:
        from django.conf import settings

        return getattr(request.session, settings.SHARDING.get('SESSION_SHARD_SELECTOR_KEY', 'shard_selector'))


class AdminShardSelector(BaseAdminShardSelector):
    @classmethod
    def retrieve_override_value(cls, request: HttpRequest) -> Any:
        return request.session.get(cls.override_shard_selector_key, None)

    @classmethod
    def set_override_value(cls, request: HttpRequest, value: Any | None) -> None:
        if value is not None:
            request.session[cls.override_shard_selector_key] = value
        else:
            request.session.pop(cls.override_shard_selector_key, None)

    @classmethod
    def format_override_label(cls, value: Any, shard: BaseShard | None, mapping: Any | None = None) -> str:
        # The parameter mapping is not used in this default implementation, but the parameter is there for user
        # specializations that may want to use it.
        if shard is not None and value is not None:
            return f'{value} \u2192 {shard.alias} ({shard.node_name}|{shard.schema_name})'
        elif shard is not None:
            return f'{shard.alias} ({shard.node_name}|{shard.schema_name})'
        else:
            return str(value)
