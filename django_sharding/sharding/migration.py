
# Make a migration forward plan

# Get migration state of all schema's

# Check the lowest of all the states

# For each forward node on the migration plan
    # For each schema
        # Migrate
        # On error, revert this node on all schema's

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations import Migration
from django.db.migrations.executor import MigrationExecutor

from sharding.utils import get_shard_class, use_shard, State


class MultiSchemaMigration(object):
    def migrate(self, schemas=[], target=None):
        """
        Migration multiple shards goes as following:
        1   determine the longest forward migration plan.
        2   Per node of the plan:
        3       Per shard:
        4           apply migration if needed
        """
        connection = connections[DEFAULT_DB_ALIAS]
        # Hook for backends needing any database preparation
        for conn in connections:
            connections[conn].prepare_database()

        executor = MigrationExecutor(connection)
        targets = executor.loader.graph.leaf_nodes()  # or target given as argument

        plan = []
        for shard in get_shard_class().objects.all():
            shard_plan = self.get_plan_for_shard(shard, targets)
            if len(shard_plan) > len(plan):
                plan = shard_plan

        for node in plan:
            for shard in get_shard_class().objects.all():
                self.check_or_migrate_shard(shard, node)

    def get_plan_for_shard(self, shard, targets):
        with use_shard(shard) as env:
            shard_executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            return shard_executor.migration_plan(targets)

    def check_or_migrate_shard(self, shard, plan_node):
        with use_shard(shard) as env:
            shard_executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            migration, backwards = plan_node
            if (migration.app_label, migration.name) in shard_executor.loader.applied_migrations:
                print(shard.alias, 'has', migration, 'already applied')
            else:
                previous_state = shard.state
                shard.state = State.MAINTENANCE
                shard.save(update_fields=['state'])

                shard_executor.migrate(targets=None, plan=[plan_node])  # no need for a target if we provide a plan

                shard.state = previous_state
                shard.save(update_fields=['state'])
