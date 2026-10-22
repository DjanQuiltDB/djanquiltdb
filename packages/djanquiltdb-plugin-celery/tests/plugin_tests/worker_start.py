import os
import subprocess
import sys

from django.test import SimpleTestCase

#: The directory of the test project, where `celery -A config.celery:app` finds the Celery app.
TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: How long the test waits for the worker to exit. A worker that starts runs until it is stopped, so the test stops it
#: after this many seconds.
WORKER_TIMEOUT = 30


class WorkerStartTestCase(SimpleTestCase):
    """
    As a worker starts, Celery's Django fixup imports run_checks, calls django.setup(), and then calls run_checks. So
    the worker calls the run_checks function that it imports before DjanQuiltDB is ready.
    """

    def test_a_check_that_raises_for_a_sharded_model_stops_the_worker(self):
        """
        Case: A Celery worker starts in a subprocess with the settings in config.worker_check_settings, which install
              the raising_check app. The check of that app raises when the router places the sharded Widget on the node
              'default'.
        Expected: The worker exits with an error and with the traceback of ShardedModelOnTheNode, because the router
                  places the sharded models on every node while the system checks of the worker run, as it does in
                  `manage.py check`.
        """
        environment = {**os.environ, 'DJANGO_SETTINGS_MODULE': 'config.worker_check_settings'}
        environment.pop('CELERY_SKIP_CHECKS', None)

        try:
            worker = subprocess.run(
                [sys.executable, '-m', 'celery', '-A', 'config.celery:app', 'worker', '--pool=solo'],
                cwd=TESTS_DIR,
                env=environment,
                capture_output=True,
                text=True,
                timeout=WORKER_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            self.fail(f'The worker is running after {WORKER_TIMEOUT} seconds, so the check does not raise.')

        self.assertNotEqual(worker.returncode, 0)
        self.assertIn('ShardedModelOnTheNode', worker.stderr)
