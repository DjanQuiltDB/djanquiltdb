from enum import Enum

__version__ = '0.1.10'

default_app_config = 'sharding.apps.ShardingConfig'


class ShardingMode(Enum):
    MIRRORED = 'M'
    SHARDED = 'S'


class State(object):
    ACTIVE = 'A'
    MAINTENANCE = 'M'


STATES = (
    (State.ACTIVE, 'Active'),
    (State.MAINTENANCE, 'Maintenance'),
)
