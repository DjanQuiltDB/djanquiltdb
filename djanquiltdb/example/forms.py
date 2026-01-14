from django.forms import ModelForm
from djanquiltdb.forms import ModelForm as ShardedModelForm

from example.models import Statement, User


class UserForm(ShardedModelForm):
    class Meta:
        model = User
        fields = ['organization', 'type']


class StatementForm(ModelForm):
    class Meta:
        model = Statement
        fields = ['user']
