from django.db import models
from django.db.models.functions import Cast
from pghistory.models import EventQuery as BaseEventQuery
from pghistory.models import EventQueryCompiler as BaseEventQueryCompiler
from pghistory.models import EventQuerySet as BaseEventQuerySet


class EventQueryCompiler(BaseEventQueryCompiler):
    """
    pghistory's proxy-field compiler, with CTE column names that work on Django 6.

    Django 6 names each column that a ``values()`` call selects after the name that was passed to ``values()``. When a
    foreign key is passed by its field name, the CTE has no ``_id`` column, but the outer query selects that column.
    This compiler passes the attname, so the CTE and the outer query use the same column names. The rest of ``_get_cte``
    is the same as in pghistory.
    """

    def _get_cte(self):
        event_model = self.query.model

        annotations = {f'_{field.column}': Cast(field.pgh_proxy, output_field=field) for field in self.proxy_fields}
        # Known gap, which pghistory 3.9 has too: values() names each CTE column after the attname, but the outer query
        # selects the column name. Only a query that selects the proxy fields of an event model goes through this CTE,
        # and such a query fails when one of the fields of the model has a db_column. A query that leaves the proxy
        # fields out, such as the ones of move_shard_to_node, works. A fix would select each field whose column differs
        # from its attname through an annotation that is named after the column. Django rejects an annotation with the
        # name of a field, so a column that is also the name of another field needs an alias that the SQL renames
        # afterwards. KnownCompilerGapTestCase covers this gap.
        values = [field.attname for field in event_model._meta.fields if not hasattr(field, 'pgh_proxy')] + [
            f'_{field.column}' for field in self.proxy_fields
        ]

        qset = models.QuerySet(event_model).annotate(**annotations).values(*values)

        sql, params = qset.query.as_sql(self, self.connection)
        with self.connection.cursor() as cursor:
            sql = cursor.mogrify(sql, params)
            # psycopg 2 and psycopg 3 return different types here.
            if isinstance(sql, bytes):  # pragma: no cover
                sql = sql.decode('utf-8')

        # Known gaps, which pghistory 3.9 has too. Both replacements below act on the whole SQL string, so they also
        # change matching text inside literals and other column names. A fix for the alias replacement would match only
        # the quoted `AS "_column"` positions.
        #
        # The operator replacement only matches `->`. A proxy field that reads more than one key compiles to `#>`, so
        # its value is returned JSON-encoded: '"ann"' and not 'ann'. Replacing more operators does not fix this, because
        # it would also break path operators that already return text. KnownCompilerGapTestCase covers this gap.
        for field in self.proxy_fields:
            sql = sql.replace(f'_{field.column}', field.column)

        sql = sql.replace('->', '->>')
        return 'WITH pgh_event_cte AS (\n' + sql + '\n)\n'


class EventQuery(BaseEventQuery):
    """
    pghistory's EventQuery, compiled by EventQueryCompiler.
    """

    def get_compiler(self, *args, **kwargs):
        compiler = super().get_compiler(*args, **kwargs)
        compiler.__class__ = EventQueryCompiler
        return compiler


class EventQuerySet(BaseEventQuerySet):
    """
    pghistory's ``EventQuerySet``, changed so that it builds the plugin's ``EventQuery``.
    """

    def __init__(self, model=None, query=None, using=None, hints=None):
        if query is None:
            query = EventQuery(model)

        super().__init__(model, query, using, hints)
