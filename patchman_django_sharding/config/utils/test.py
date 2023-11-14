import sys

from django.test.runner import DiscoverRunner
from django import get_version


class WildcardDiscoverRunner(DiscoverRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = '*.py'


try:
    # noinspection PyUnresolvedReferences
    from teamcity.unittestpy import TeamcityTestRunner, TeamcityTestResult
except ImportError:
    TeamcityRunner = WildcardDiscoverRunner
else:
    class TestResult(TeamcityTestResult):
        @staticmethod
        def get_test_id_with_description(test):
            test_suffix = (f'.python-{"-".join(str(x) for x in sys.version_info[:2])}'
                           f'-django-{get_version().replace(".", "-")}')
            result = TeamcityTestResult.get_test_id_with_description(test)
            return result + test_suffix

    class TestRunner(TeamcityTestRunner):
        resultclass = TestResult

    class TeamcityRunner(WildcardDiscoverRunner):
        test_runner = TestRunner
