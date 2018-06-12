from django.forms.models import ModelFormMetaclass as _ModelFormMetaclass, ModelForm as _ModelForm


class ModelFormMetaClass(_ModelFormMetaclass):
    def __new__(mcs, name, bases, attrs):
        new_class = super().__new__(mcs, name, bases, attrs)

        # We don't want to save the shard options when the class will be loaded
        for field in new_class.base_fields.values():
            if hasattr(field, 'queryset') and hasattr(field.queryset, '_shard'):
                delattr(field.queryset, '_shard')

        return new_class


class ModelForm(_ModelForm, metaclass=ModelFormMetaClass):
    pass
