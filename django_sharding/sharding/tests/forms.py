import importlib

from django.test import TestCase

from example.models import Shard
from sharding import State
from sharding.utils import create_template_schema, use_shard


class ModelFormTestCase(TestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(alias='death_star', schema_name='test_empire_schema', node_name='default',
                                          state=State.ACTIVE)

    def test_queryset_shard_options(self):
        """
        Case: Import two forms: one that inherits from sharding.forms.ModelForm and one that inherits from
              django.forms.ModelForm
        Expected: For the class definition, the base_fields don't have a _shard attribute set for the form that
                  inherits from sharding.forms.ModelForm and do have _shard set for forms that inherits from
                  django.forms.ModelForm. The same goes for the fields attribute on the form instances.
        """
        with use_shard(self.shard):
            # Import it in a sharded context to force having the _shard attribute set for forms that don't inherit from
            # our sharding.forms.ModelForm. We reload it explicitly to make sure that previous imports in our tests are
            # undone and that we have the _shard attribute set.
            import example.forms
            importlib.reload(example.forms)

        # No _shard set on the queryset
        self.assertFalse(hasattr(example.forms.UserForm.base_fields['organization'].queryset, '_shard'))
        self.assertFalse(hasattr(example.forms.UserForm.base_fields['type'].queryset, '_shard'))

        # _shard set on the queryset
        self.assertTrue(hasattr(example.forms.StatementForm.base_fields['user'].queryset, '_shard'))

        # And the same goes for form instances
        form = example.forms.UserForm()

        self.assertFalse(hasattr(form.fields['organization'].queryset, '_shard'))
        self.assertFalse(hasattr(form.fields['type'].queryset, '_shard'))

        second_form = example.forms.StatementForm()

        self.assertTrue(hasattr(second_form.fields['user'].queryset, '_shard'))
