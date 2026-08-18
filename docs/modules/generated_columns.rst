=================
Generated columns
=================

Postgres can compute a column from the other columns of its row and store the result, using
``GENERATED ALWAYS AS (...) STORED``. Sharded tables support these, but a stored generation expression needs care that
an ordinary column does not, because it is bound to the function it calls rather than merely naming it.

Postgres 18 adds ``VIRTUAL`` generated columns, which compute the value on read instead of storing it. These are
supported out of the box and need nothing from this chapter. Postgres refuses a user-defined function in a virtual
generation expression, so such a column can only be built from built-ins, which is also why the shard cloning described
below deliberately leaves virtual columns alone.


Declaring a generated column
----------------------------

A generation expression usually calls a function, and that function has to exist in the database before the column
referencing it can be created. Create it in a migration that runs before the one adding the column;
:doc:`database_functions` covers how, and which schema to put it in.

Reference it using Django's ``GeneratedField``, naming the function unqualified so that each shard resolves it through
its own search path:

.. code-block:: python

    # example/models.py
    from django.db import models
    from django.db.models import F, Func

    from djanquiltdb.decorators import sharded_model


    @sharded_model()
    class Cake(models.Model):
        name = models.CharField('name', max_length=128)
        name_uppercased = models.GeneratedField(
            expression=Func(F('name'), function='alluppercase'),
            output_field=models.TextField(),
            db_persist=True,
        )


Which function the clone binds to
---------------------------------

Postgres stores a generation expression with the function's OID rather than its name, and shard creation clones a
schema with ``CREATE TABLE ... (LIKE source INCLUDING ALL)``, which copies that OID as it stands. A clone would
therefore inherit a reference to the *specific function object* the template resolved, so cloning is followed by a
pass that rebinds those expressions by name: each is read back from the template with only the template on the search
path, and applied again with the new shard first on it.

A generation expression consequently ends up on the same function the equivalent query would find from that shard. A
``SHARDED`` function is copied into every shard, and each shard's generated column calls its own copy; a ``PUBLIC``
function has a single copy in the public schema, and every shard's generated column keeps calling that one. This is
the rule :doc:`database_functions` describes, and the rebinding is what makes the stored expression agree with it.

Three other kinds of expression are rebound in the same pass and follow the same rule:

* Column defaults, which is also what re-points a ``nextval()`` default at the sequence created for this schema.
  Identity columns stay clear of this by themselves, an identity not being a default.
* ``CHECK`` constraints, which have no ``ALTER ... SET`` form and are therefore dropped and re-added. A side effect is
  that the clone ends up carrying the template's constraint names rather than ones Postgres invented for it.
* Expression and partial indexes, likewise recreated, and likewise under the template's names.


Costs and limits
~~~~~~~~~~~~~~~~

Rebinding a stored generated column rewrites the table to recompute its values, at a cost proportional to the rows the
template carries. A template that is kept small is cheap to clone from.

An expression belonging to an exclusion constraint is not rebound, since such a constraint owns its index and cannot
be replaced piecemeal. Keep those off functions that live in the template schema, so that there is nothing per-shard
for them to be bound to in the first place.


Moving data between shards
--------------------------

A generated column is never written, only computed, so the commands that move rows between schemas leave it out of the
copy entirely: ``move_data_to_shard`` and ``move_shard_to_node`` name the copyable columns explicitly on both sides of
the transfer. The target computes its own values with its own expression, which is the same one the rebinding pass
gave it.


Public functions reaching sharded tables
----------------------------------------

Note that a function can contain references to sharded tables even if it's stored in the public schema. If those table
names do not have hardcoded schema qualifications in the function definition, they will resolve to the tables inside the
appropriate schema for the table holding the generated column in question at execution time.

This is why it is **generally advisable** to create a function for a generated column as *public*, even if the generated
column is on a *sharded* model. Each table in each shard can safely reference the single public function, without
needing to clone the function to each shard and requiring a rewrite pass as described above.
