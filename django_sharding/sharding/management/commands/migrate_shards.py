# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import itertools
import time
import traceback
import warnings
from collections import OrderedDict
from django.conf import settings
from importlib import import_module

from django.apps import apps
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.core.management.color import no_style
from django.core.management.sql import (
    custom_sql_for_model, emit_post_migrate_signal, emit_pre_migrate_signal,
)
from django.db import DEFAULT_DB_ALIAS, connections, router, transaction, connection
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state import ProjectState
from django.utils.deprecation import RemovedInDjango110Warning
from django.utils.module_loading import module_has_submodule
from django.core.management.commands.migrate import Command as MigrateCommand

from sharding.utils import get_shard_class, use_shard, State, get_template_name, migrate_schema


class Command(MigrateCommand):
    """
    migrate_shards extends the normal migration command.
    The main handle function is entirely replaced, but most of it is similar.
    Major differences:
        A plan is made for each shard and the longest is executed.
        It is executed per node.
            And each node is done for every shard and template before moving to the next node.
        Same for fake and reverse operations.
    """
    help = "Updates database schema. Manages both apps with migrations and those without."

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)
        parser._option_string_actions['--database'].default = 'all'
        parser._option_string_actions['--database'].help = \
            'Nominates a database to synchronize. Defaults to all databases.'
        parser._option_string_actions['--database'].choices = ['all'] + self.get_all_but_replica_dbs()

    def get_all_but_replica_dbs(self):
        return list(filter(
            lambda db: not settings.DATABASES[db].get('PRIMARY', None),
            settings.DATABASES.keys()
        ))

    def handle(self, *args, **options):
        self.verbosity = options.get('verbosity', 0)
        self.interactive = options.get('interactive')
        self.show_traceback = options.get('traceback')
        self.load_initial_data = options.get('load_initial_data')
        self.database_options = options.get('database')

        # Import the 'management' module within each installed app, to register
        # dispatcher events.
        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, "management"):
                import_module('.management', app_config.name)

        # Get the database we're operating from
        #
        if not self.database_options or self.database_options == 'all':
            databases = self.get_all_but_replica_dbs()
        elif self.database_options not in self.get_all_but_replica_dbs():
            raise ValueError('You must migrate an existing non-primary DB.')
        else:
            databases = [self.database_options]

        if options.get("list", False):
            self.stderr.write(
                "The 'migrate --list' command is not supported in django_sharding. Use 'showmigrations' instead.")

        for conn in connections:
            connections[conn].prepare_database()

        executor = MigrationExecutor(connection)

        # Before anything else, see if there's conflicting apps and drop out
        # hard if there are any
        conflicts = executor.loader.detect_conflicts()
        if conflicts:
            name_str = "; ".join(
                "%s in %s" % (", ".join(names), app)
                for app, names in conflicts.items()
            )
            raise CommandError(
                "Conflicting migrations detected (%s).\nTo fix them run "
                "'python manage.py makemigrations --merge'" % name_str
            )

        # If they supplied command line arguments, work out what they mean.
        target_app_labels_only = True
        if options.get('app_label') and options.get('migration_name'):
            app_label, migration_name = options['app_label'], options['migration_name']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '%s' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)" % app_label
                )
            if migration_name == "zero":
                targets = [(app_label, None)]
            else:
                try:
                    migration = executor.loader.get_migration_by_prefix(app_label, migration_name)
                except AmbiguityError:
                    raise CommandError(
                        "More than one migration matches '%s' in app '%s'. "
                        "Please be more specific." %
                        (migration_name, app_label)
                    )
                except KeyError:
                    raise CommandError("Cannot find a migration matching '%s' from app '%s'." % (
                        migration_name, app_label))
                targets = [(app_label, migration.name)]
            target_app_labels_only = False
        elif options.get('app_label'):
            app_label = options['app_label']
            if app_label not in executor.loader.migrated_apps:
                raise CommandError(
                    "App '%s' does not have migrations (you cannot selectively "
                    "sync unmigrated apps)" % app_label
                )
            targets = [key for key in executor.loader.graph.leaf_nodes() if key[0] == app_label]
        else:
            targets = executor.loader.graph.leaf_nodes()

        # Work out from which node we need to migrate
        plan = executor.migration_plan(targets)  # for default|public
        template_name = get_template_name()
        for shard in get_shard_class().objects.filter(node_name__in=databases):
            shard_plan = self.get_plan_for_shard(shard.node_name, shard.schema_name, targets)
            if len(shard_plan) > len(plan):
                plan = shard_plan
        for db in databases:  # do templates and publics
            public_plan = self.get_plan_for_shard(db, 'public', targets)
            if len(public_plan) > len(plan):
                plan = public_plan

            template_plan = self.get_plan_for_shard(db, template_name, targets)
            if len(template_plan) > len(plan):
                plan = template_plan

        # Execute the plan
        emit_pre_migrate_signal([], self.verbosity, self.interactive, connection.alias)

        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Running migrations:"))

        if not plan:
            executor.check_replacements()
            if self.verbosity >= 1:
                self.stdout.write("  No migrations to apply.")
                # If there's changes that aren't in migrations yet, tell them how to fix it.
                autodetector = MigrationAutodetector(
                    executor.loader.project_state(),
                    ProjectState.from_apps(apps),
                )
                changes = autodetector.changes(graph=executor.loader.graph)
                if changes:
                    self.stdout.write(self.style.NOTICE(
                        "  Your models have changes that are not yet reflected "
                        "in a migration, and so won't be applied."
                    ))
                    self.stdout.write(self.style.NOTICE(
                        "  Run 'manage.py makemigrations' to make new "
                        "migrations, and then re-run 'manage.py migrate' to "
                        "apply them."
                    ))
        else:
            fake = options.get("fake")
            fake_initial = options.get("fake_initial")
            for node in plan:
                # migrate all public schemas and templates
                for db in databases:
                    self.check_or_migrate_schema(db, 'public', node, fake, fake_initial)
                    self.check_or_migrate_schema(db, template_name, node, fake, fake_initial)

                # migrate all shards
                for shard in get_shard_class().objects.filter(node_name__in=databases):
                    self.check_or_migrate_shard(shard, node, fake, fake_initial)

        emit_post_migrate_signal([], self.verbosity, self.interactive, connection.alias)

    def get_plan_for_shard(self, node_name, schema_name, targets):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            shard_executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            return shard_executor.migration_plan(targets)

    def check_or_migrate_schema(self, node_name, schema_name, plan_node, fake, fake_initial):
        with use_shard(node_name=node_name, schema_name=schema_name) as env:
            executor = MigrationExecutor(env.connection, self.migration_progress_callback)
            migration, backwards = plan_node
            if (not (migration.app_label, migration.name) in executor.loader.applied_migrations) == backwards:
                if self.verbosity >= 2:
                    if backwards:
                        "    {}|{} does not have {} applied yet.\n".format(node_name, schema_name, migration)
                    else:
                        "    {}|{} has {} already applied.\n".format(node_name, schema_name, migration)

            else:
                if self.verbosity >= 1:
                    self.stdout.write(
                        "    {} {} to default|public\n".format('Unapplying' if fake else 'Applying', migration)
                    )
                executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)

    def check_or_migrate_shard(self, shard, plan_node, fake, fake_initial):
        with use_shard(shard) as env:
            shard_executor = MigrationExecutor(env.connection)
            migration, backwards = plan_node
            if (not (migration.app_label, migration.name) in shard_executor.loader.applied_migrations) == backwards:
                if self.verbosity >= 2:
                    if backwards:
                        "    {}|{} does not have {} applied yet.\n".format(migration, shard.node_name, shard.alias)
                    else:
                        "    {}|{} has {} already applied.\n".format(migration, shard.node_name, shard.alias)

            else:
                previous_state = shard.state
                shard.state = State.MAINTENANCE
                shard.save(update_fields=['state'])

                shard_executor.migrate(targets=None, plan=[plan_node], fake=fake, fake_initial=fake_initial)

                shard.state = previous_state
                shard.save(update_fields=['state'])
