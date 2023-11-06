from enum import Enum

__version__ = '2.0.0'


default_app_config = 'sharding.apps.ShardingConfig'


class ShardingMode(Enum):
    MIRRORED = 'M'
    PUBLIC = 'P'
    SHARDED = 'S'


public_modes = (ShardingMode.MIRRORED, ShardingMode.PUBLIC)


class State(object):
    ACTIVE = 'A'
    MAINTENANCE = 'M'


STATES = (
    (State.ACTIVE, 'Active'),
    (State.MAINTENANCE, 'Maintenance'),
)
