from django.contrib.auth.models import AbstractBaseUser, UserManager
from django.db import models
from django.utils import timezone

from sharding.decorators import mirrored_model, sharded_model


# mirrored table
@mirrored_model()
class Type(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'example'


# lead sharded table
@sharded_model()
class Organization(models.Model):
    name = models.CharField('name', max_length=100)
    created_at = models.DateTimeField('created at', default=timezone.now)

    class Meta:
        app_label = 'example'


# child sharded table
@sharded_model()
class User(AbstractBaseUser):
    def get_full_name(self):
        return self.name

    def get_short_name(self):
        return self.name

    name = models.CharField('name', max_length=100)
    email = models.EmailField('email address', unique=True)
    created_at = models.DateTimeField('date joined', default=timezone.now)
    organization = models.ForeignKey('Organization', verbose_name='organization')
    type = models.ForeignKey('Type', verbose_name='type')

    USERNAME_FIELD = 'email'

    objects = UserManager()

    class Meta:
        app_label = 'example'

    def __str__(self):
        return self.name
