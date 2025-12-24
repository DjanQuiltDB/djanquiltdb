======
Celery
======

Celery shard unavailable decorator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Celery tasks too need to be aware on which shard to do what. If the shard is unavailable during the execution of a view,
the middleware is triggered to deal with the raised exception. To do the same for Celery tasks we suggest the following
decorator.

It is not supplied as standard by this library for we don't want to add celery as a requirement.

.. code-block:: python

    from celery import shared_task
    from celery.exceptions import MaxRetriesExceededError
    from djanquiltdb.utils import use_shard, StateException

    from example.models import User
    from example.models import OrganizationShards  # organization->shard mapping model


    class HandleStateException(object):

        func = None

        def __init__(self, replace_exception=None, retry_delay=None, max_retries=15):
            self.replace_exception = replace_exception
            self.retry_delay = retry_delay
            self.max_retries = max_retries

        def __call__(self, *args, **kwargs):
            if self.func is None:
                self.func = args[0]
                return self
            try:
                return self.func(*args, **kwargs)
            except StateException as e:
                if self.retry_delay:
                    try:
                        self.func.retry(countdown=self.retry_delay, max_retries=self.max_retries)
                    except MaxRetriesExceededError:
                        print('{} task failed after {} tries.'.format(self.func, self.max_retries))  # use a logger
                else:
                    if self.replace_exception:
                        raise self.replace_exception
                    else:
                        raise e


    @HandleStateException(retry_delay=2)
    @shared_task()
    def do_something(organization_id):
        shard = OrganizationShards.objects.get(organization_id=organization_id).shard
        with use_shard(shard):
            for user in User.objects.filter(organization_id=organization_id):
                print("user:", user)
