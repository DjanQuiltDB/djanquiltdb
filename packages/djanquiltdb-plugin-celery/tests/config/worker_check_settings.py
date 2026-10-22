"""
The settings of the test project plus the raising_check app, for the test that starts a Celery worker in a subprocess.
"""

from config.settings import *  # noqa: F403

INSTALLED_APPS = (*INSTALLED_APPS, 'raising_check')  # noqa: F405
