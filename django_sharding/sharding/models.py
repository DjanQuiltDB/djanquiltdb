from django.db import models


class BaseShard(models.Model):
    alias = models.CharField(max_length=128, db_index=True)
    db_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars

    class Meta:
        app_label = 'sharding'
        abstract = True


class BaseNode(models.Model):
    uri = models.CharField(max_length=128)

    class Meta:
        app_label = 'sharding'
        abstract = True