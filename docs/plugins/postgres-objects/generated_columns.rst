==================
Generated columns
==================

A declarative function can be used in a generated column, which Django supports natively with
``django.db.models.GeneratedField``. However, you can also use ``postgres_objects.GeneratedField`` to make the field
automatically recalculate pre-existing stored values at the moment you change a Function.

What this plugin offers is the awareness of propagating those recalculation operations to the correct location based
on sharding decorators. For example, a public function used in sharded generated columns will cause the function to be
updated in public, but recalculation to occur on each shard individually.

For more information on how ``postgres_objects.GeneratedField`` works, refer to
`django-postgres-objects' own documentation
<https://django-postgres-objects.readthedocs.io/en/latest/modules/queries.html#generated-columns>`_.
