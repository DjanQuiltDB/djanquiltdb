from django.forms import ModelForm

from example.models import User, Statement
from sharding.forms import ModelForm as ShardedModelForm


class UserForm(ShardedModelForm):
    class Meta:
        model = User
        fields = ['organization', 'type']


class StatementForm(ModelForm):
    class Meta:
        model = Statement
        fields = ['user']
