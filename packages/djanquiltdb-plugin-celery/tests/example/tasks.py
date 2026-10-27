from celery import shared_task
from djanquiltdb.decorators.celery import (
    shared_task_for_active_shard,
    shared_task_for_every_node,
    shared_task_for_every_shard,
    shared_task_for_node,
    shared_task_for_shard,
)
from djanquiltdb.router import get_active_connection

from config.celery import app
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


@shared_task_for_shard(bind=True)
def record_target(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_shard(bind=True, max_retries=1, retry_backoff=2)
def record_target_with_one_retry(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_shard(bind=True, lock=False)
def record_target_without_lock(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_shard
def create_widget(name, **kwargs):
    return Widget.objects.create(name=name).pk


@shared_task_for_shard
def record_schema_and_shard(shard=None):
    return {'schema_name': get_active_connection().schema_name, 'shard': shard}


@shared_task_for_active_shard
def record_received_shard_id(shard_id):
    return shard_id


@shared_task_for_shard
def record_received_tenant_id(tenant_id=None):
    return tenant_id


@shared_task_for_shard
def record_schema_of_shard_id(shard_id):
    return [get_active_connection().schema_name, shard_id]


@shared_task_for_shard
def record_schema_of_tenant(tenant_id, label):
    return [get_active_connection().schema_name, tenant_id, label]


@shared_task_for_node(bind=True)
def record_node(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_node
def record_received_node_name(node_name):
    return node_name


@shared_task_for_active_shard(bind=True)
def record_inherited_target(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_shard(bind=True, inherit_active_shard=True)
def record_target_that_inherits(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_active_shard
def create_widget_in_active_shard(name, **kwargs):
    return Widget.objects.create(name=name).pk


@shared_task_for_every_shard(bind=True)
def record_every_shard(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_every_shard(bind=True, queue='fanout', spread=(1, 2), include_maintenance=True)
def record_every_shard_on_the_fanout_queue(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_every_shard
def record_shard_id(shard_id):
    active_connection = get_active_connection()

    return {
        'node_name': active_connection.node_name,
        'schema_name': active_connection.schema_name,
        'received': shard_id,
    }


@shared_task_for_every_shard
def record_shard_id_and_days(shard_id, days):
    return {'received': shard_id, 'days': days}


@shared_task_for_every_shard
def record_schema_of_every_shard():
    return get_active_connection().schema_name


@shared_task_for_every_node(bind=True)
def record_every_node(self, **kwargs):
    return describe_task_run(self, kwargs)


@shared_task_for_every_node
def record_node_name(node_name, **kwargs):
    active_connection = get_active_connection()

    return {
        'node_name': active_connection.node_name,
        'schema_name': active_connection.schema_name,
        'received': node_name,
    }


# A chord needs the results of the tasks in its header, so aggregate_per_shard() stores them even for a task that is
# decorated with ignore_result=True.
@shared_task_for_shard(ignore_result=True)
def count_widgets(**kwargs):
    return Widget.objects.count()


@shared_task
def collect_counts(counts):
    return counts


@app.task_for_shard(bind=True)
def record_target_of_the_app(self, **kwargs):
    return describe_task_run(self, kwargs)


@app.task_for_node(bind=True)
def record_node_of_the_app(self, **kwargs):
    return describe_task_run(self, kwargs)


@app.task_for_active_shard(bind=True)
def record_inherited_target_of_the_app(self, **kwargs):
    return describe_task_run(self, kwargs)


@app.task_for_every_shard(bind=True)
def record_every_shard_of_the_app(self, **kwargs):
    return describe_task_run(self, kwargs)


@app.task_for_every_node(bind=True)
def record_every_node_of_the_app(self, **kwargs):
    return describe_task_run(self, kwargs)
