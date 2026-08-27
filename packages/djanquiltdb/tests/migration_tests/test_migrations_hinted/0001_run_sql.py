"""
RunSQL carrying its placement as a hint, which is how a statement that is bound to no model says where it belongs.
"""

from django.db import migrations

from djanquiltdb import ShardingMode


class Migration(migrations.Migration):
    initial = True

    operations = [
        migrations.RunSQL(
            'CREATE TABLE hinted_public (id serial PRIMARY KEY)',
            reverse_sql='DROP TABLE hinted_public',
            hints={'sharding_mode': ShardingMode.PUBLIC},
        ),
        migrations.RunSQL(
            'CREATE TABLE hinted_sharded (id serial PRIMARY KEY)',
            reverse_sql='DROP TABLE hinted_sharded',
            hints={'sharding_mode': ShardingMode.SHARDED},
        ),
    ]
