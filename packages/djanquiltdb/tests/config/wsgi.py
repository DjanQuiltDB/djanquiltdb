"""
WSGI config for djanquiltdb project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/howto/deployment/wsgi/
"""

from django.core.wsgi import get_wsgi_application

from config.settings import set_settings_module

set_settings_module()

application = get_wsgi_application()
