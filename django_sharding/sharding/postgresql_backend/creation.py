from django.conf import settings
from django.db.backends.postgresql_psycopg2.creation import DatabaseCreation as BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Extend this method to create a template schema as well during test database creation. Note that the
        create_test_db would be a better place to put this in, but we do want the template schema to be created before
        we serialize the database, to make sure everything in the template schema will be serialized as well. We can do
        that in create_test_db, but that requires us to copy over everything that's in there, instead of simply hooking
        into this method.

        Note that testing this change is a big challenge, due to the fact that test db creation is done at the start of
        the test run, and calling create_test_db again will interfere with the current test run. Django itself has also
        minimal test coverage for this, and mostly test error paths.
        """
        test_database_name = super()._create_test_db(verbosity, autoclobber, keepdb=keepdb)

        # This is actually done in the `create_test_db`, but we need it now to be sure that we create a template schema
        # on the correct database.
        settings.DATABASES[self.connection.alias]['NAME'] = test_database_name
        self.connection.settings_dict['NAME'] = test_database_name

        if not keepdb:
            from sharding.utils import create_template_schema  # Prevent cyclic imports

            create_template_schema(
                node_name=self.connection.alias,
                verbosity=max(verbosity - 1, 0),
                migrate=False  # Will be done in the migrate command
            )

        return test_database_name
