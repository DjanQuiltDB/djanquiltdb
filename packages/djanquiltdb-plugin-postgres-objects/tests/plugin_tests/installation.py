from django.test import SimpleTestCase
from djanquiltdb import ShardingMode
from postgres_objects.base import DeclarativeObject

from djanquiltdb_plugin_postgres_objects import install


class InstallTestCase(SimpleTestCase):
    def setUp(self):
        super().setUp()
        original = DeclarativeObject.router_hints
        self.addCleanup(setattr, DeclarativeObject, 'router_hints', original)

    def test_install_respects_hints_something_else_already_set(self):
        """
        Case: install() runs while ``DeclarativeObject.router_hints`` already holds a placement.
        Expected: The pre-set placement survives. The default is only for declarations nothing has placed, so a project
                  that sets its own baseline keeps it.
        """
        DeclarativeObject.router_hints = {'sharding_mode': ShardingMode.SHARDED}

        install()

        self.assertEqual(DeclarativeObject.router_hints, {'sharding_mode': ShardingMode.SHARDED})

    def test_install_is_idempotent(self):
        """
        Case: install() runs twice over an empty baseline, as it does when something re-populates the app registry.
        Expected: The default PUBLIC placement is in place afterwards, set the first time and left alone the second.
        """
        DeclarativeObject.router_hints = {}

        install()
        install()

        self.assertEqual(DeclarativeObject.router_hints, {'sharding_mode': ShardingMode.PUBLIC})
