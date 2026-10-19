from djanquiltdb.decorators.celery import task_for_active_shard, task_for_node, task_for_shard
from djanquiltdb.router import get_active_connection

from djanquiltdb_plugin_celery.targets import TARGET_HEADER_NAME
from example.models import Widget


def describe_task_run(task, kwargs):
    """
    Returns where a task runs and whether its shard is locked, the keyword arguments that the task receives, and the
    target header and the number of retries of its request.
    """
    active_connection = get_active_connection()

    return {
        'node_name': active_connection.node_name,
        'schema_name': active_connection.schema_name,
        'lock': active_connection.lock,
        'kwargs': kwargs,
        'header': (task.request.headers or {}).get(TARGET_HEADER_NAME),
        'retries': task.request.retries,
    }


@task_for_shard(bind=True)
def record_target(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_shard(bind=True, max_retries=1, retry_backoff=2)
def record_target_with_one_retry(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_shard(bind=True, lock=False)
def record_target_without_lock(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_shard
def create_widget(name, **kwargs):
    return Widget.objects.create(name=name).pk


@task_for_shard
def record_schema_and_shard(shard=None):
    return {'schema_name': get_active_connection().schema_name, 'shard': shard}


@task_for_active_shard
def record_received_shard_id(shard_id):
    return shard_id


@task_for_shard
def record_received_tenant_id(tenant_id=None):
    return tenant_id


@task_for_node(bind=True)
def record_node(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_node
def record_received_node_name(node_name):
    return node_name


@task_for_active_shard(bind=True)
def record_inherited_target(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_shard(bind=True, inherit_active_shard=True)
def record_target_that_inherits(self, **kwargs):
    return describe_task_run(self, kwargs)


@task_for_active_shard
def create_widget_in_active_shard(name, **kwargs):
    return Widget.objects.create(name=name).pk
