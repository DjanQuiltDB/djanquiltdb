from typing import TYPE_CHECKING, Optional

from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured

if TYPE_CHECKING:
    from djanquiltdb.contrib.quilt_admin.shard_selector import BaseAdminShardSelector

ADMIN_SHARD_SELECTOR_CLASS: Optional[BaseAdminShardSelector] = None


class ShardedAdminAppConfig(AppConfig):
    name = 'djanquiltdb.contrib.quilt_admin'
    verbose_name = 'DjanQuiltDB Admin Integration'

    def ready(self):
        from django.conf import settings
        from django.utils.module_loading import import_string

        from djanquiltdb.contrib.quilt_admin.shard_selector import BaseAdminShardSelector

        global ADMIN_SHARD_SELECTOR_CLASS

        if hasattr(settings, 'QUILT_ADMIN') and 'SHARD_SELECTOR_CLASS' in settings.QUILT_ADMIN:
            shard_selector_class = settings.QUILT_ADMIN['SHARD_SELECTOR_CLASS']
        else:
            shard_selector_class = 'djanquiltdb.contrib.quilt_admin.shard_selector.AdminShardSelector'

        ADMIN_SHARD_SELECTOR_CLASS = import_string(shard_selector_class)

        if not issubclass(ADMIN_SHARD_SELECTOR_CLASS, BaseAdminShardSelector):
            raise ImproperlyConfigured(
                f"Shard selector class {ADMIN_SHARD_SELECTOR_CLASS} doesn't subclass BaseAdminShardSelector"
            )
