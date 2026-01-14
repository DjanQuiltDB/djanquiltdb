from django.test.runner import DiscoverRunner


class WildcardDiscoverRunner(DiscoverRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = '*.py'
