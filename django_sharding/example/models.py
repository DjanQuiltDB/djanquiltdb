from django.contrib.auth.models import AbstractBaseUser, BaseUserManager
from django.db import models, IntegrityError
from django.utils import timezone

from sharding import State, STATES
from sharding.decorators import mirrored_model, sharded_model, shard_mapping_model, public_model
from sharding.models import BaseShard, MappingQuerySet


__all__ = [
    'Shard',
    'OrganizationShards',
    'SuperType',
    'Type',
    'Organization',
    'Suborganization',
    'Cake',
    'ProxyCake',
    'User',
    'MirroredUser',
    'DefaultUser',
    'Statement',
]


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


class SuperTypeManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@public_model()
class SuperType(models.Model):
    name = models.CharField('name', max_length=100)

    objects = SuperTypeManager()

    class Meta:
        app_label = 'example'
        unique_together = [['name']]

    def natural_key(self):
        return self.name,


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

    def delete(self, force=False):
        if not force:
            raise IntegrityError('Cannot delete a cake, you should eat it!')

        return super().delete()


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
        app_label = 'example'


class UserManager(BaseUserManager):
    def _create_user(self, email, password, is_staff, **extra_fields):
        """
        Creates and saves a User with the given username, email and password.
        """
        now = timezone.now()
        if not email:
            raise ValueError('The given email must be set')
        email = self.normalize_email(email)
        user = self.model(
            email=email,
            is_staff=is_staff,
            is_active=True,
            created_at=now,
            **extra_fields
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_user(self, email, password=None, **extra_fields):
        return self._create_user(email, password, False, **extra_fields)

    def create_superuser(self, email, password, **extra_fields):
        return self._create_user(email, password, True, **extra_fields)


class AbstractUser(AbstractBaseUser):
    name = models.CharField('name', max_length=100)
    email = models.EmailField('email address', unique=True)

    created_at = models.DateTimeField('date joined', default=timezone.now)
    is_staff = models.BooleanField('staff status', default=False,
                                   help_text='Designates whether the user can log into this admin site.')
    is_active = models.BooleanField('active', default=True,
                                    help_text='Designates whether this user should be treated as active. '
                                              'Unselect this instead of deleting accounts.')

    USERNAME_FIELD = 'email'

    objects = UserManager()

    class Meta:
        abstract = True

    def __str__(self):
        return self.name

    def get_full_name(self):
        return self.name

    def get_short_name(self):
        return self.name


@sharded_model()
class User(AbstractUser):
    organization = models.ForeignKey('Organization', verbose_name='organization', null=True)
    type = models.ForeignKey('Type', on_delete=models.DO_NOTHING, verbose_name='type', null=True)
    cake = models.ManyToManyField('Cake', verbose_name='cakes')

    USERNAME_FIELD = 'email'

    objects = UserManager()

    class Meta:
        app_label = 'example'

    def get_organization_name(self):
        """ For testing purposes, we do a new query here to get the organization name """
        return Organization.objects.get(id=self.organization_id).name


@mirrored_model()
class MirroredUser(AbstractUser):
    class Meta:
        app_label = 'example'


class DefaultUser(AbstractUser):
    """ User that's not sharded nor mirrored. Used for the `createsuperuser` test """

    class Meta:
        app_label = 'example'


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
