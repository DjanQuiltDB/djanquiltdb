=====
Views
=====

To prevent the need to use ``with use_shard():`` in every view your project has, this library provides a middleware to do that for you.

The ``UseShardMiddleware`` will enable a use_shard context manager in ``process_request`` and close it in ``process_response`` or ``process_exception``.

.. image:: view_flow.svg
   :scale: 100 %
   :alt: Django Sharding request flow
   :align: center

Since it inherits ``StateExceptionMiddleware`` It will raise a 503 error if the shard required is in a non-active state.

See :ref:`use_shard_middleware` for details about using this middleware.
