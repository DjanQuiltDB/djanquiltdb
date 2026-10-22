"""
The initial migration of the app, under the name that the test database records as applied, so that ``migrate`` skips
it and the next migration of this module runs on tables that exist already.
"""

from importlib import import_module

Migration = import_module('migration_tests.migrations.0001_migrations_test_init').Migration
