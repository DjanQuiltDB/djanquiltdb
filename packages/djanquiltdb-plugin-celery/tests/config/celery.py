from djanquiltdb.base.celery import QuiltCelery

app = QuiltCelery('config')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
