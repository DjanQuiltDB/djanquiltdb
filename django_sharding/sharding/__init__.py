from enum import Enum

__version__ = '0.4.6'

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
