from django.apps import AppConfig


class DjanQuiltDBOAuthToolkitConfig(AppConfig):
    """
    The app of the plugin, which only exists so that its ``cleartokens`` management command replaces the toolkit's. List
    it before ``oauth2_provider`` in ``INSTALLED_APPS``. The rest of the plugin works without it.
    """

    name = 'djanquiltdb_plugin_oauth_toolkit'
    verbose_name = 'DjanQuiltDB for django-oauth-toolkit'
