from djanquiltdb.utils import use_shard, use_shard_for

"""
Proxy objects that ensure that even if the current request is on a different shard than where the user is stored,
it can still be used for lookups of e.g. name and permissions. This is needed to make the Quilt Admin render
contents for a selected shard while applying permissions based on the user as defined in its own shard.
"""


class CrossShardUserProxy:
    def __init__(self, user, shard):
        self._user = user
        self._shard = shard

    def __getattr__(self, name):
        with use_shard(self._shard):
            return getattr(self._user, name)


class CrossShardMappingUserProxy:
    def __init__(self, user, shard):
        self._user = user
        self._shard = shard

    def __getattr__(self, name):
        with use_shard_for(self._shard):
            return getattr(self._user, name)
