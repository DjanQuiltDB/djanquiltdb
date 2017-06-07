from django.db import models

from sharding.models import BaseShard, BaseNode


class Node(BaseNode):
    class Meta:
        app_label = 'shardingtest'


class Shard(BaseShard):
    node = models.ForeignKey(Node, on_delete=models.PROTECT)

    class Meta:
        app_label = 'shardingtest'

