=====================================
Running tasks in a shard or on a node
=====================================

The plugin's decorators turn a function into a Celery task that runs in one shard, with ``shared_task_for_shard``, or in
the public schema of one node, with ``shared_task_for_node``. That shard or schema is the *target* of the task.


Sending a task to a shard
-------------------------

``shared_task_for_shard`` runs a task in a shard. ``for_shard()`` takes the shard separately from the arguments of the
task: a shard, its primary key, or a mapping value that is passed as the keyword argument for a mapping value. It
returns an object with the methods of a task that send it: ``delay()``, ``apply_async()``, ``delay_on_commit()``,
``apply_async_on_commit()``, ``s()``, ``si()`` and ``signature()``. Each of these methods takes the same arguments as
the method of the task.

.. code-block:: python

    from djanquiltdb.decorators.celery import shared_task_for_shard


    @shared_task_for_shard
    def send_invoice(invoice_id):
        Invoice.objects.get(pk=invoice_id).send()


    send_invoice.for_shard(shard).delay(invoice_id=12)
    send_invoice.for_shard(tenant_id=3).apply_async(kwargs={'invoice_id': 12}, countdown=60)

``delay_for_shard()``, ``apply_async_for_shard()``, ``delay_on_commit_for_shard()`` and
``apply_async_on_commit_for_shard()`` do the same in one call. They take a shard or its primary key as their first
argument, and the arguments of the method without ``_for_shard`` after it:

.. code-block:: python

    send_invoice.delay_for_shard(shard, invoice_id=12)
    send_invoice.apply_async_for_shard(shard, kwargs={'invoice_id': 12}, countdown=60)

The keyword arguments of the task can identify the shard too:

* ``shard_id`` identifies a shard by its primary key.
* The mapping field of the ``MAPPING_MODEL``, such as ``tenant_id``, identifies a shard by its mapping value. A
  mapping value, such as the id of a tenant, is a value of that field, and the ``MAPPING_MODEL`` maps it to a
  shard. The task enters that shard with ``use_shard_for()``, which also checks the state of the mapping. In a project
  that has a ``MAPPING_MODEL``, ``QUILT_CELERY['MAPPING_KWARG']`` can be set to another keyword argument for the mapping
  value.

.. code-block:: python

    send_invoice.delay(invoice_id=12, tenant_id=3)

A keyword argument whose value is None does not identify a shard. ``node_name`` is an ordinary keyword argument for a
task of ``shared_task_for_shard``, and does not identify its target.

A function that declares ``shard_id`` or the keyword argument for a mapping value can also receive it by position, as
any argument of a function can be passed, so ``send_invoice_of_tenant(tenant_id, invoice_id)`` is sent as
``send_invoice_of_tenant.delay(3, 12)``. A task that runs once in every shard is the exception: its fan-out sends
``shard_id`` by keyword, so a positional argument is never its target.

Sending the task without a shard, or with two shards, raises ``TaskShardError`` at the call, before anything is
published. A task that is sent through ``for_shard()`` and has a shard in its arguments as well has two shards. When the
keyword arguments have a ``node_name``, the message of the error points at ``shared_task_for_node``. The decorator takes
the options of ``celery.shared_task()``, such as ``name``, ``queue`` and ``bind``. A function that is decorated with
``bind=True`` receives the task as its first argument, as with Celery's own decorator.

Calling the task as a function, as ``send_invoice(invoice_id=12, tenant_id=3)``, runs the function in the shard of
its keyword arguments too, without sending a message.


What the function receives
--------------------------

The function receives a keyword argument that identifies the target only when the function declares that keyword
argument by name. ``**kwargs`` does not receive it. So ``send_invoice(invoice_id)`` above takes no ``shard_id`` and no
``tenant_id``. When the task is sent, its arguments are checked against the function in the form in which the function
receives them, so ``send_invoice.delay(invoice_id=12, tenant_id=3)`` passes the check, and
``send_invoice.delay(12, 13)`` raises ``TypeError``. A task that is decorated with ``typing=False`` is sent without this
check, as with Celery's own decorator.

A function that declares ``shard_id`` receives the primary key of the shard that the task runs in, also when the task is
sent through ``for_shard()``, with a mapping value, or in the active shard. A function that declares the keyword
argument for a mapping value receives the mapping value that is sent, or, when no mapping value is sent, the default of
its parameter:

.. code-block:: python

    @shared_task_for_shard
    def rebuild_search_index(shard_id):
        SearchIndex.rebuild(shard_id)


    rebuild_search_index.delay_for_shard(shard)  # receives shard.pk


Sending a task to a node
------------------------

``shared_task_for_node`` runs a task in the public schema of a node: the node that ``for_node()`` is given, or the node
that the ``node_name`` keyword argument identifies. ``delay_for_node()``, ``apply_async_for_node()``,
``delay_on_commit_for_node()`` and ``apply_async_on_commit_for_node()`` take the name of the node as their first
argument:

.. code-block:: python

    from djanquiltdb.decorators.celery import shared_task_for_node


    @shared_task_for_node
    def refresh_exchange_rates():
        ExchangeRate.objects.refresh()


    refresh_exchange_rates.for_node('other').delay()
    refresh_exchange_rates.delay_for_node('other')
    refresh_exchange_rates.delay(node_name='other')

The function receives ``node_name`` only when it declares it by name, and then also when the task is sent through
``for_node()``. Sending the task without a node raises ``TaskNodeError``. When the keyword arguments identify a shard,
the message of the error points at ``shared_task_for_shard``. ``shard_id`` and the keyword argument for a mapping value
are ordinary keyword arguments for a task of ``shared_task_for_node``. The decorator takes the options of
``celery.shared_task()``, as ``shared_task_for_shard`` does, but no ``lock``, because the public schema of a node has
none. ``for_shard()`` on a task of ``shared_task_for_node``, and ``for_node()`` on a task that runs in a shard, raise
``TypeError``.

``TaskShardError`` and ``TaskNodeError`` both inherit from ``TaskQuiltError``, so ``except TaskQuiltError`` catches a
task without a target of either kind. All three are imported from ``djanquiltdb.utils.celery``.


The target in the message
-------------------------

The task resolves its target when the task is sent, in ``apply_async()`` and ``apply()``, or when its signature is
built, in ``signature()``, ``s()`` and ``si()``. The target is sent in the ``djanquiltdb_target`` message header. Every
way of sending a task goes through one of these methods: ``delay()``, a group, a chain, a chord, a link callback and a
retry too. The worker enters the target in the header, so the task runs in its shard or node on a worker thread on
which no shard is active.

A signature needs its target when the signature is built. The worker sends a later task of a chain, the body of a chord
and a link callback after the task before them has run, and the shard of the sender is not active on the worker.
Building a signature without a target raises ``TaskShardError`` or ``TaskNodeError``, so that a canvas fails before
anything is published:

.. code-block:: python

    chain(import_invoices.si(tenant_id=3), send_invoices.si(tenant_id=3)).delay()  # both run in the shard
    chain(import_invoices.si(tenant_id=3), send_invoices.si()).delay()  # TaskShardError

A message that is not sent through the task, such as a message from ``app.send_task('myapp.tasks.send_invoice', ...)``,
has no target header. The worker then reads the target from the keyword arguments of the message, and fails with
``TaskShardError`` or ``TaskNodeError`` when they do not identify one.


Shards in maintenance
---------------------

A shard that is being created, moved or purged is in maintenance, and ``use_shard()`` raises ``StateException`` for it.
The task is then retried with an exponential backoff: after 5, 10, 20, 40 and 60 seconds by default. After the last
retry it fails with the ``StateException``. The ``max_retries``, ``retry_backoff``, ``retry_backoff_max`` and
``retry_jitter`` options of the decorator set the retries of one task, and the ``QUILT_CELERY`` settings set the
defaults:

.. code-block:: python

    @shared_task_for_shard(max_retries=20, retry_backoff=10, retry_backoff_max=600)
    def rebuild_search_index():
        ...

The plugin retries the task with Celery's ``autoretry_for`` option, and adds ``StateException`` to the exceptions that
are passed to the decorator in ``autoretry_for``. So a ``StateException`` that the function raises itself, such as for
another mapping value that is in maintenance, is retried too.

A task that runs eagerly, with ``task_always_eager``, is only retried when ``task_eager_propagates`` is off. When
``task_eager_propagates`` is on, the caller receives Celery's ``Retry`` exception, and the task is not retried. The
retries of an eager task run at once, without their countdown. A task that is called as a function raises the
``StateException`` without a retry.


The lock of the shard
---------------------

The task enters its shard with ``use_shard()``, or with ``use_shard_for()`` for a mapping value. As a request does, the
task holds the shared advisory lock of the shard, and of the mapping value, while it runs. ``move_data_to_shard`` and
``move_shard_to_node`` take the exclusive lock before they put the shard in maintenance, so these commands wait for the
tasks that run in the shard. A task that starts while one of these commands holds the exclusive lock waits for that
lock. Once the task holds the shared lock, the shard is read again. When the shard is in maintenance at that point, or
has moved, ``StateException`` is raised, and the task is retried. A task of ``shared_task_for_node`` holds no lock,
because a public schema has none. ``lock=False`` turns the lock off for a task that has to run while these commands run:

.. code-block:: python

    @shared_task_for_shard(lock=False)
    def record_page_view(page_id):
        ...


Inheriting the active shard
---------------------------

A task of ``shared_task_for_active_shard`` that is sent without a shard runs in the shard that is active where the task
is sent. The active shard is the shard of ``use_shard()``, of ``use_shard_for()`` or of a request that the DjanQuiltDB
middleware wraps. ``shared_task_for_shard(inherit_active_shard=True)`` does the same.

.. code-block:: python

    from djanquiltdb.decorators.celery import shared_task_for_active_shard


    @shared_task_for_active_shard
    def recount_invoices():
        Invoice.objects.recount()


    def invoice_view(request):
        ...
        recount_invoices.delay()  # runs in the shard of the request
        recount_invoices.delay_for_shard(archive_shard)  # runs in archive_shard

So ``delay()`` and ``apply_async()`` need no shard inside ``use_shard()``. The shard of ``for_shard()`` and a shard in
the keyword arguments take precedence over the active shard. Sending the task without a shard while no shard is active
raises ``TaskShardError``, so that the task does not run in a shard that nobody chose. The public schema of a node is
not a shard, so a task that is sent inside ``use_shard(node_name=..., schema_name='public')`` raises ``TaskShardError``
too. A signature that is built inside ``use_shard()`` gets the active shard as its target, so a chain that is built in a
request runs each of its tasks in the shard of the request. Calling the task as a function inside ``use_shard()`` runs
the function in the active shard too.

``delay_on_commit()`` and ``apply_async_on_commit()``, also those of ``for_shard()``, resolve the target of the task
when they are called. They send the task once the transactions that are open at the call have committed, on every node,
and at once when no transaction is open. So the task runs in the shard that is active at the call, also when the
transaction commits after ``use_shard()`` has exited. The call raises ``TaskShardError`` at once when there is no shard.
Both methods also wait for a transaction that is opened on one node before ``use_shard()`` enters a shard on another
node. When one of the transactions rolls back, the task is not sent.
