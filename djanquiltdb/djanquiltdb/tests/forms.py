import importlib

from example.models import Shard
from djanquiltdb import State
from djanquiltdb.tests.utils import ShardingTestCase
from djanquiltdb.utils import create_template_schema, use_shard


class ModelFormTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(alias='death_star', schema_name='test_empire_schema', node_name='default',
                                          state=State.ACTIVE)

    def test_queryset_shard_options(self):
        """
        Case: Import two forms: one that inherits from djanquiltdb.forms.ModelForm and one that inherits from
              django.forms.ModelForm
        Expected: For the class definition, the base_fields don't have a _shard_options hint attribute set for the form
                  that inherits from djanquiltdb.forms.ModelForm and do have _shard_options set for forms that inherits
                  from django.forms.ModelForm. The same goes for the fields attribute on the form instances.
        """
        with use_shard(self.shard):
            # Import it in a sharded context to force having the _shard attribute set for forms that don't inherit from
            # our sharding.forms.ModelForm. We reload it explicitly to make sure that previous imports in our tests are
            # undone and that we have the _shard attribute set.
            import example.forms
            importlib.reload(example.forms)

        # No _shard_options set as hint on the queryset
        self.assertNotIn('_shard_options', example.forms.UserForm.base_fields['organization'].queryset._hints)
        self.assertNotIn('_shard_options', example.forms.UserForm.base_fields['type'].queryset._hints)

        # _shard_options set as hint on the queryset
        self.assertIn('_shard_options', example.forms.StatementForm.base_fields['user'].queryset._hints)

        # And the same goes for form instances
        form = example.forms.UserForm()

        self.assertNotIn('_shard_options', form.fields['organization'].queryset._hints)
        self.assertNotIn('_shard_options', form.fields['type'].queryset._hints)

        second_form = example.forms.StatementForm()

        self.assertIn('_shard_options', second_form.fields['user'].queryset._hints)
