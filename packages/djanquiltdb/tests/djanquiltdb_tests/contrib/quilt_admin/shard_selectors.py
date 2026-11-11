from djanquiltdb.contrib.quilt_admin.shard_selector import AdminShardSelector
from djanquiltdb.db import connection
from example.models import OrganizationShard, Shard


class EvenOrganizationsShardSelector(AdminShardSelector):
    """
    Lets the user switch to the mappings of even organization ids only, standing in for a project that limits a user to
    some tenants.
    """

    @classmethod
    def switchable_mappings(cls, request, queryset):
        return queryset.filter(organization_id__in=[2, 4])


class EvenAliasShardSelector(AdminShardSelector):
    """
    Lets the user switch to the shards with the aliases shard_2 and shard_4 only, standing in for a project without a
    mapping model that limits a user to some shards.
    """

    @classmethod
    def switchable_shards(cls, request, queryset):
        return queryset.filter(alias__in=['shard_2', 'shard_4'])


class QuerysetIgnoringShardSelector(AdminShardSelector):
    """
    Builds its own querysets of organization 2 and shard_2 instead of filtering the querysets that it receives.
    """

    @classmethod
    def switchable_mappings(cls, request, queryset):
        return OrganizationShard.objects.filter(organization_id=2)

    @classmethod
    def switchable_shards(cls, request, queryset):
        return Shard.objects.filter(alias='shard_2')


class SlicingShardSelector(AdminShardSelector):
    """Caps the shard switcher at one entry with a slice."""

    @classmethod
    def switchable_mappings(cls, request, queryset):
        return queryset.order_by('organization_id')[:1]

    @classmethod
    def switchable_shards(cls, request, queryset):
        return queryset.order_by('alias')[:1]


class RecordingShardSelector(AdminShardSelector):
    """
    Records the user of the request and the schema of the active connection on each call of switchable_mappings() and
    switchable_shards(), in ``calls``.
    """

    calls = []

    @classmethod
    def switchable_mappings(cls, request, queryset):
        cls.calls.append((request.user, connection.schema_name))
        return queryset

    @classmethod
    def switchable_shards(cls, request, queryset):
        cls.calls.append((request.user, connection.schema_name))
        return queryset
