from django.forms.models import ModelFormMetaclass, ModelForm as BaseModelForm


class ModelFormMetaClass(ModelFormMetaclass):
    def __new__(mcs, name, bases, attrs):
        new_class = super().__new__(mcs, name, bases, attrs)

        for field in new_class.base_fields.values():
            if hasattr(field, 'queryset') and hasattr(field.queryset, '_shard'):
                delattr(field.queryset, '_shard')

        return new_class


class ModelForm(BaseModelForm, metaclass=ModelFormMetaClass):
    pass
