import os


def set_settings_module():
    """
    Sets the DJANGO_SETTINGS_MODULE environment variable to that defined in
    the 'settings_module' file.

    If the `settings_module` file is missing or empty, the
    DJANGO_SETTINGS_MODULE environment variable is not set.
    """

    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.dev')
