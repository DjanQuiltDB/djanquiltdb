from djanquiltdb.postgresql_backend.base import ShardDatabaseWrapper


def patch_in_lookup():
    from django.db.models.lookups import In

    _original_process_rhs = In.process_rhs

    def shardoptions_aware_process_rhs(self, compiler, connection):
        """
        In django.db.models.lookups.In.process_rhs(), validation occurs to check that the nested lookup is executed
        against the same database connection as the main query. This traditionally breaks because it compares the _db
        property of the subquery (which is ShardOptions) against the alias of the connection passed in (which is str).
        This could be fixed by modifying ShardOptions.__eq__, but this is risky since it can mask errors in cases where
        we should be comparing all ShardOptions properties instead of purely the connection alias.

        In order to only allow this comparison only in this one known (and verified as safe) context, we can trigger a
        switch on ShardDatabaseWrapper which makes it temporarily pretend that the ShardOptions are its alias, causing
        the comparison to succeed. This switch is then disabled again as soon as process_rhs() is done, which means by
        the time the SQL is executed, the connection.alias property is back to being a regular string. This minimizes
        the scope of this allowed comparison only to a sequence that has been validated to have no side effects.
        """
        from djanquiltdb.options import ShardOptions

        present_shard_options_as_alias = isinstance(getattr(self.rhs, '_db', None), ShardOptions) and isinstance(
            connection, ShardDatabaseWrapper
        )

        if present_shard_options_as_alias:
            connection._present_shard_options_as_alias = True

        # Call the original process_rhs() method with the potentially modified connection
        result = _original_process_rhs(self, compiler, connection)

        if present_shard_options_as_alias:
            connection._present_shard_options_as_alias = False

        return result

    # Add the as_postgresql method to the In class
    In.process_rhs = shardoptions_aware_process_rhs


# Automatically patch when this module is imported
patch_in_lookup()
