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
        # Known gap, shared with pghistory 3.9: a field with a db_column has an attname differing from the column,
        # so the values() aliasing names the CTE column after the attname while the outer query selects the
        # column. A complete fix would select every field through a Cast annotation named after its column, the
        # way the proxy fields already are.
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

        # Known gaps, shared with pghistory 3.9: both replacements rewrite the whole SQL string. The alias rewrite
        # would have to anchor on the quoted `AS "_column"` positions not to also rewrite a literal or another
        # column containing the same text, and the operator rewrite breaks chained JSON paths, where only the
        # last `->` may become `->>`, as well as `->` appearing inside a literal.
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
