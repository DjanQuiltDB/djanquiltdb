from django.contrib.auth.models import AbstractBaseUser, BaseUserManager
from django.db import models
from djanquiltdb import STATES, State
from djanquiltdb.decorators import mirrored_model, shard_mapping_model, sharded_model
from djanquiltdb.models import BaseQuiltSession, BaseShard, MappingQuerySet


@mirrored_model()
class Shard(BaseShard):
    class Meta:
        app_label = 'example'


# Maps each organization onto the shard that has its data. The organization id is the shard selector in the client ids,
# the tokens and the session keys.
@mirrored_model()
@shard_mapping_model(mapping_field='organization_id')
class OrganizationShard(models.Model):
    shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
    organization_id = models.PositiveIntegerField(unique=True)
    state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

    objects = MappingQuerySet.as_manager()

    class Meta:
        app_label = 'example'


class UserManager(BaseUserManager):
    def create_user(self, email, password=None):
        user = self.model(email=self.normalize_email(email))
        user.set_password(password)
        user.save(using=self._db)
        return user


# Each organization has its own users, so a user id is only unique inside a shard.
@sharded_model()
class User(AbstractBaseUser):
    email = models.EmailField('email address', unique=True)
    is_active = models.BooleanField('active', default=True)

    USERNAME_FIELD = 'email'

    objects = UserManager()

    class Meta:
        app_label = 'example'


@sharded_model()
class QuiltSession(BaseQuiltSession):
    class Meta:
        app_label = 'example'
