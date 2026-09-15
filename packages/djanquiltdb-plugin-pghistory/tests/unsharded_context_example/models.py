"""
Public and mirrored models whose triggers need a Context table and a _pgh_attach_context() function that are not
sharded. config.settings_public_context and config.settings_mirrored_context install this app, and config.settings,
in which the Context table is sharded, does not.
"""

import pghistory
from django.db import models
from djanquiltdb.decorators import mirrored_model, public_model
from djanquiltdb.decorators.pghistory import track_mirrored, track_public


@mirrored_model()
@track_mirrored(context_field=pghistory.ContextForeignKey())
class Kitchen(models.Model):
    name = models.CharField('name', max_length=128)


class RecipeManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@track_public(context_field=pghistory.ContextForeignKey())
@public_model(allow_copy=False)
class Recipe(models.Model):
    name = models.CharField('name', max_length=100)

    objects = RecipeManager()

    class Meta:
        unique_together = [['name']]

    def natural_key(self):
        return (self.name,)


# The statement-level triggers of a mirrored and a public model call _pgh_attach_context() from outside the shards.
@mirrored_model()
@track_mirrored(level=pghistory.Statement)
class Proofer(models.Model):
    name = models.CharField('name', max_length=128)


class GrainManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@track_public(level=pghistory.Statement)
@public_model(allow_copy=False)
class Grain(models.Model):
    name = models.CharField('name', max_length=100)

    objects = GrainManager()

    class Meta:
        unique_together = [['name']]

    def natural_key(self):
        return (self.name,)
