"""
A plugin for the tests of the command hooks, whose hooks record when they run.
"""

import contextlib
from types import SimpleNamespace
from unittest import mock


def recording_plugin(section, context_name, hook_names, record):
    """
    Returns a plugin object with one section of hooks. Each hook calls record() with its name and its arguments, and
    the context hook does so when it is entered ('enter <name>') and when it is exited ('exit <name>').
    """

    @contextlib.contextmanager
    def context_hook(*args):
        record('enter ' + context_name, *args)
        yield
        record('exit ' + context_name, *args)

    hooks = {name: (lambda *args, hook_name=name: record(hook_name, *args)) for name in hook_names}

    return SimpleNamespace(**{section: SimpleNamespace(**{context_name: context_hook}, **hooks)})


def plugin_installed(plugin):
    """
    Makes plugin the only loaded plugin while the block runs.
    """
    return mock.patch('djanquiltdb.plugins.load_plugins', return_value=(plugin,))
