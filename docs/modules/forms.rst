==========
Forms
==========

It can happen that when you import a model form in a sharded context, and that form contains fields that have foreign
keys to sharded models, it will save the shard on the queryset. This can cause unseen problems when using the form
multiple times and the queryset being evaluated in a shard you don't expect. Therefore, we need to skip saving the
shard options on the queryset during constructing the form class.

When using a model form, you can inherit from ``sharding.forms.ModelForm`` instead of the default Django
``django.forms.ModelForm``:

.. code-block:: python

    from example.models import User
    from sharding.forms import ModelForm


    class UserForm(ShardedModelForm):
        class Meta:
            model = User
            fields = ['organization', 'type']


You can also use the ``sharding.forms.ModelFormMetaClass`` metaclass if you don't use the default Django model form.
For example, when using floppyforms:

.. code-block:: python

    import floppyforms
    from example.models import User
    from sharding.forms import ModelFormMetaClass


    class FloppyModelForm(floppyforms.ModelForm, metaclass=ModelFormMetaClass):
        pass


    class UserForm(FloppyModelForm):
        class Meta:
            model = User
            fields = ['organization', 'type']
