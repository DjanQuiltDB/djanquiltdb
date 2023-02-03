from django.test.runner import DiscoverRunner


class WildcardDiscoverRunner(DiscoverRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = '*.py'


try:
    # noinspection PyUnresolvedReferences
    from teamcity.unittestpy import TeamcityTestRunner
except ImportError:
    TeamcityRunner = WildcardDiscoverRunner
else:
    class TeamcityRunner(WildcardDiscoverRunner):
        test_runner = TeamcityTestRunner
