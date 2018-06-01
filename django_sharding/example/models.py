from django.contrib.auth.models import AbstractBaseUser, UserManager
from django.db import models
from django.utils import timezone

from sharding import State, STATES
from sharding.decorators import mirrored_model, sharded_model, shard_mapping_model
from sharding.models import BaseShard, MappingQuerySet


class Shard(BaseShard):

    class Meta:
        app_label = 'example'


# mapping table
@shard_mapping_model(mapping_field='organization_id')
class OrganizationShards(models.Model):
    shard = models.ForeignKey('example.Shard')
    organization_id = models.PositiveSmallIntegerField()
    state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)
    slug = models.SlugField()

    objects = MappingQuerySet.as_manager()


@mirrored_model()
class SuperType(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'example'


@mirrored_model()
class Type(models.Model):
    name = models.CharField('name', max_length=100)
    super = models.ForeignKey('SuperType', on_delete=models.DO_NOTHING, verbose_name='super', null=True)

    class Meta:
        app_label = 'example'


# Lead sharded table
@sharded_model()
class Organization(models.Model):
    name = models.CharField('name', max_length=100)
    created_at = models.DateTimeField('created at', default=timezone.now)

    class Meta:
        app_label = 'example'

    def __str__(self):
        return self.name

    def get_all_descendants(self):
        result = []
        suborganizations = list(Suborganization.objects.filter(parent=self))
        for suborganization in suborganizations:
            suborganizations.extend(list(Suborganization.objects.filter(parent=suborganization.child)))
            result.append(suborganization.child)

        return result


@sharded_model()
class Suborganization(models.Model):
    parent = models.ForeignKey('Organization', verbose_name='organization', related_name='parent')
    child = models.OneToOneField('Organization', verbose_name='organization', related_name='children')

    class Meta:
        app_label = 'example'


class CakeQuerySet(models.QuerySet):
    def chocolate(self):
        return self.filter(name__icontains='chocolate')


@sharded_model()
class Cake(models.Model):
    name = models.CharField('name', max_length=128)

    objects = CakeQuerySet.as_manager()

    class Meta:
        app_label = 'example'

    def __str__(self):
        return self.name


@sharded_model()
class ProxyCake(Cake):
    class Meta:
        proxy = True


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
    type = models.ForeignKey('Type', on_delete=models.DO_NOTHING, verbose_name='type', null=True)
    cake = models.ManyToManyField('Cake', verbose_name='cakes')

    USERNAME_FIELD = 'email'

    objects = UserManager()

    class Meta:
        app_label = 'example'

    def __str__(self):
        return self.name

    def get_organization_name(self):
        """ For testing purposes, we do a new query here to get the organization name """
        return Organization.objects.get(id=self.organization_id).name


@sharded_model()
class Statement(models.Model):
    content = models.CharField('content', max_length=300)
    user = models.ForeignKey('User', verbose_name='user')
    type = models.ManyToManyField('Type', verbose_name='types')
    offset = models.PositiveIntegerField('offset', blank=True, null=True)  # Field name is a postgres reserved word

    class Meta:
        app_label = 'example'

    def __str__(self):
        return '{}: {}'.format(self.user.name, self.content)
