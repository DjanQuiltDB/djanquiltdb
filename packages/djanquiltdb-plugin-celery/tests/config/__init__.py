# Django imports this package before config.settings, so the Celery app is created when Django loads the settings. Every
# shared task of the project then uses this app, as the Celery documentation for Django describes.
from config.celery import app as celery_app

__all__ = ['celery_app']
