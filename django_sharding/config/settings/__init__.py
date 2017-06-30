import os


def get_settings_module():
    if os.path.isfile('settings_module'):
        with open('settings_module', 'r') as f:
            return f.read().strip()


def set_settings_module():
    """
    Sets the DJANGO_SETTINGS_MODULE environment variable to that defined in
    the 'settings_module' file.

    If the `settings_module` file is missing or empty, the
    DJANGO_SETTINGS_MODULE environment variable is not set.
    """
    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        settings_module = get_settings_module()

        if settings_module:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_module)
