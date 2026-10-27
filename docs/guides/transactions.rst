============
Transactions
============

Django's ``transaction.atomic()`` opens a transaction on one connection. A sharded project has a connection for each
node, and a write can go to another node than the one that is active: the router sends every write to a mirrored model
to the primary node, the node that ``PRIMARY_DB_ALIAS`` names (see :doc:`/reference/settings`). DjanQuiltDB's
``atomic()`` takes that into account.


``transaction.atomic()`` in a sharded project
---------------------------------------------

When DjanQuiltDB's app is ready, it replaces ``django.db.transaction.atomic`` with ``djanquiltdb.transaction.atomic``,
so ``transaction.atomic()`` in a view, a task or a management command is DjanQuiltDB's. It works as Django's does,
with these differences:

* **With** ``using``, it is Django's ``atomic()``: the transaction is on the connection that ``using`` names.
* **Without** ``using``, the transaction is on the connection that is active when the block is entered, such as the
  connection of the shard of ``use_shard()``. That includes a function decorated with the bare ``@transaction.atomic``,
  for which Django's own ``atomic()`` always takes the ``default`` connection.
* **On another node than the primary node**, a block without ``using`` opens a transaction on the primary node as well,
  so that a write to a mirrored model, which goes to the primary node, and a write to the active shard commit or roll
  back together::

      from django.db import transaction

      with use_shard_for('acme'), transaction.atomic():
          invoice = Invoice.objects.create(...)  # Sharded: the node of the shard of acme.
          InvoiceNumber.objects.create(...)  # Mirrored: the primary node.

  The two transactions commit one after the other, the active node first. Without two-phase commit, a failure between
  the two commits leaves the first one committed.

On the primary node, or in a project with one node, both transactions are the same one, and ``atomic()`` opens one
transaction, as Django's does.

``atomic()`` decides whether to open the transaction on the primary node as well when it is called. In
``with transaction.atomic():`` that is when the block is entered. A decorator is called when the function is decorated,
which is usually when its module is imported and no shard is active, so a function decorated with
``@transaction.atomic`` opens its transaction on the connection that is active when it runs, and never on the primary
node as well. Use ``with transaction.atomic():`` inside the function for a block that writes a mirrored model from a
shard on another node.


When to import ``djanquiltdb.transaction.atomic``
-------------------------------------------------

The replacement happens when DjanQuiltDB's app is ready, and Django imports the models of every app before that. Code
that takes ``atomic`` from Django while it is imported, before that moment, keeps Django's own:

* ``from django.db.transaction import atomic`` in a models module, or in a module that a models module imports;
* a ``@transaction.atomic`` decorator on a function or method in such a module, because the decorator is applied when
  the module is imported.

Django's ``atomic()`` there opens its transaction on one connection only: the bare decorator on the ``default``
connection, whatever shard is active, and ``atomic()`` with parentheses on the active connection, without the primary
node. Import DjanQuiltDB's ``atomic()`` in those modules instead, so that a decorated function opens its transaction on
the connection that is active when it runs, such as the shard of the instance that a method of a sharded model runs
in::

    from djanquiltdb.transaction import atomic


    @sharded_model()
    class Invoice(models.Model):
        @atomic
        def book(self):
            ...

``transaction.atomic()`` that is looked up when the code runs, such as ``with transaction.atomic():`` in the body of a
function, always finds DjanQuiltDB's.


Enter the shard first
---------------------

``atomic()`` opens its transaction on the connection that is active when the block is entered. Enter the shard before
the transaction, so that the transaction is on the connection of the shard::

    with use_shard(shard), transaction.atomic():
        ...

A transaction that is opened before ``use_shard()`` is on the connection that was active then.


Transactions on several nodes
-----------------------------

``transaction_for_nodes(nodes=[...])`` opens a transaction on each of the nodes that it is given, and
``transaction_for_every_node()`` on every node in ``DATABASES``. Both are in ``djanquiltdb.utils``. They open the
transactions in the order of the nodes, and close them in the reverse order. When one of the nodes fails, every node
rolls back, unless its transaction was committed already. ``lock_models`` takes pairs of a model and a PostgreSQL lock
mode, such as ``((Type, 'ACCESS EXCLUSIVE'),)``, and locks the table of each model on every node.

Both are durable by default: they open the outermost transaction, and raise ``RuntimeError`` inside another atomic
block.

The ``@atomic_write_to_every_node`` decorator runs a function once for every node, in the public schema of the node,
inside ``transaction_for_every_node()``. :doc:`/reference/api` describes these, and ``djanquiltdb.transaction``.
