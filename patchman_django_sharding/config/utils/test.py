import pluggy
from django.test.runner import DiscoverRunner


class WildcardDiscoverRunner(DiscoverRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = '*.py'


try:
    # noinspection PyUnresolvedReferences
    from teamcity.unittestpy import TeamcityTestRunner
    from teamcity import is_running_under_teamcity
    from teamcity.messages import TeamcityServiceMessages
except ImportError:
    TeamcityRunner = WildcardDiscoverRunner
else:
    class TeamcityRunner(WildcardDiscoverRunner):
        test_runner = TeamcityTestRunner


    hookimpl = pluggy.HookimplMarker("tox")

    _messages = TeamcityServiceMessages()


    def _testsuite_name(venv):
        return venv.name


    @hookimpl
    def tox_runtest_pre(venv):
        if is_running_under_teamcity():
            _messages.testSuiteStarted(_testsuite_name(venv))


    @hookimpl
    def tox_runtest_post(venv):
        if is_running_under_teamcity():
            _messages.testSuiteFinished(_testsuite_name(venv))
