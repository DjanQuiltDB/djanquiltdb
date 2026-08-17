from djanquiltdb.middleware import BaseUseShardMiddleware


class UseShardMiddleware(BaseUseShardMiddleware):
    def get_shard_id(self, request):
        return request.session.get('shard_id')
