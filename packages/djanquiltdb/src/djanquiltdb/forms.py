from django.forms.models import ModelForm as _ModelForm
from django.forms.models import ModelFormMetaclass as _ModelFormMetaclass


class ModelFormMetaClass(_ModelFormMetaclass):
    def __new__(mcs, name, bases, attrs):
        new_class = super().__new__(mcs, name, bases, attrs)

        # We don't want to save the shard options when the class will be loaded
        for field in new_class.base_fields.values():
            if hasattr(field, 'queryset'):
                field.queryset._hints.pop('_shard_options', None)

        return new_class


class ModelForm(_ModelForm, metaclass=ModelFormMetaClass):
    """
    Django's ``ModelForm``, minus the routing a related field's queryset captured at class definition time.

    A form class is built once, on whichever shard happened to be active then, and a ``ModelChoiceField`` would hold
    on to that shard for the life of the process. Subclass this one instead and each request's queryset is routed
    where that request is.
    """

    pass
