"""
Taken, changed and adopted from:
    https://github.com/bernardopires/django-tenant-schemas/blob/master/tenant_schemas/postgresql_backend/base.py
Credits goes to bernardopires

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""

import logging
import re

from django.conf import settings
from django.db.backends.base.base import NO_DB_ALIAS
from django.db.backends.postgresql.base import DatabaseWrapper as BaseDatabaseWrapper
from django.db.utils import DatabaseError, IntegrityError
from django.utils.module_loading import import_string
from psycopg import sql
from psycopg.errors import InternalError

from djanquiltdb.postgresql_backend.introspection import DatabaseSchemaIntrospection
from djanquiltdb.postgresql_backend.utils import CursorDebugWrapper, CursorWrapper

logger = logging.getLogger(__name__)

# Clone function is from the PostgreSQL wiki by Emanuel '3manuek'.
# Adjusted to set the value of the created sequences to the same value as those we clone.
clone_schema_function = """
CREATE OR REPLACE FUNCTION public.clone_schema(source_schema TEXT, dest_schema TEXT) RETURNS VOID AS
$BODY$
DECLARE
  dest_table TEXT;
  dest_table_path TEXT;
  seq_name TEXT;
  tbl_name TEXT;
  ident_rec_ RECORD;
  src_seq_ TEXT;
  dest_seq_ TEXT;
  last_val_ BIGINT;
  is_called_ BOOLEAN;
  trigger_defs_ TEXT[];
  trigger_def_ TEXT;
  func_def TEXT;
  header_kw_ TEXT;
  header_prefix_ TEXT;
  copyable_columns_ TEXT;
  rebind_stmts_ TEXT[];
  rebind_drops_ TEXT[];
  rebind_adds_ TEXT[];
  rebind_stmt_ TEXT;
  entry_search_path_ TEXT;

BEGIN
  /* SET LOCAL survives function return until the transaction ends, so remember the caller's search_path: a caller
   * running inside an open transaction (a Shard saved under transaction.atomic()) must not keep resolving unqualified
   * names against the schemas this function switches to.
   */
  entry_search_path_ := current_setting('search_path');

  /* Set search_path to include source_schema and public, so that unqualified references in the statements below
   * resolve against the schema being cloned and then against public, the same way the application sees them.
   */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema) || ',public,pg_catalog';

  /* Create all sequences that exist on the source schema on the target schema. */
  FOR dest_table IN
    SELECT sequence_name::text FROM information_schema.SEQUENCES WHERE sequence_schema = source_schema
  LOOP
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS ' || dest_schema || '.' || dest_table;
    /* Set sequence value based on source sequence last_value.
     * After tables are cloned, we'll update sequences to ensure they're higher than any existing IDs.
     */
    EXECUTE format('SELECT setval(%L, (SELECT last_value FROM %I.%I), (SELECT is_called FROM %I.%I))',
      dest_schema || '.' || dest_table, source_schema, dest_table, source_schema, dest_table);
  END LOOP;

  /* Only base tables are copied here (views are handled separately below) */
  FOR dest_table IN
    SELECT TABLE_NAME::text FROM information_schema.TABLES
      WHERE table_schema = source_schema AND table_type = 'BASE TABLE'
  LOOP
    dest_table_path := dest_schema || '.' || dest_table;
    /* Create all tables on the target schema. */
    EXECUTE 'CREATE TABLE ' || dest_table_path || ' (LIKE ' || source_schema || '.' || dest_table || ' INCLUDING ALL)';

    /* Copy over rows naming each column explicitly to avoid errors on generated columns (i.e. without SELECT *). Keep
     * this predicate in sync with DatabaseWrapper.get_copyable_column_names and get_copyable_column_names_by_table.
     */
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO copyable_columns_
      FROM pg_catalog.pg_attribute
      WHERE attrelid = format('%I.%I', source_schema, dest_table)::regclass
        AND attnum > 0
        AND NOT attisdropped
        AND attgenerated = '';

    IF copyable_columns_ IS NOT NULL THEN
      EXECUTE 'INSERT INTO ' || dest_table_path || ' (' || copyable_columns_ || ')'
        || ' SELECT ' || copyable_columns_ || ' FROM ' || source_schema || '.' || dest_table;
    END IF;
  END LOOP;

  /* Clone all functions from the source schema to the destination schema.
   * This must be done after tables are cloned, because functions may reference tables.
   * This is also necessary because triggers (cloned below) may reference functions in the same schema.
   *
   * Only the header's schema qualification is adapted, matched as an exact prefix, so the body is never touched:
   * an unqualified reference in a body resolves through the caller's search_path at runtime, per schema, while an
   * explicitly qualified one (or a string literal that happens to contain a schema name) survives untouched.
   * Aggregates are skipped: pg_get_functiondef cannot render them, so they are not carried into clones.
   */
  FOR func_def IN
    SELECT pg_get_functiondef(p.oid) AS func_def
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = source_schema AND p.prokind IN ('f', 'p', 'w')
  LOOP
    FOREACH header_kw_ IN ARRAY ARRAY['FUNCTION', 'PROCEDURE'] LOOP
      header_prefix_ := 'CREATE OR REPLACE ' || header_kw_ || ' ' || quote_ident(source_schema) || '.';
      IF left(func_def, length(header_prefix_)) = header_prefix_ THEN
        func_def := 'CREATE OR REPLACE ' || header_kw_ || ' ' || quote_ident(dest_schema) || '.'
          || substr(func_def, length(header_prefix_) + 1);
        EXIT;
      END IF;
    END LOOP;
    EXECUTE func_def;
  END LOOP;

  /* CREATE TABLE ... (LIKE ... INCLUDING ALL) above copies parsed expression trees, so every expression that calls a
   * function keeps the source schema's function OID, and every default reading a sequence keeps the source schema's
   * sequence OID. Column defaults, generated columns, CHECK constraints and expression or partial indexes on the
   * clone would therefore keep using the source schema's objects rather than the copies made for this schema.
   *
   * Rebind them by name. Render each expression with only the source schema on the search_path: pg_get_expr() and
   * friends schema-qualify a reference when it is invisible on the current path, so same-schema references print
   * unqualified while references into other schemas (public, most notably) print qualified. Re-applying that text
   * with the destination schema first on the path then resolves the unqualified names against this schema's own
   * copies while leaving the qualified ones alone. This is the same principle the view cloning below relies on, and
   * like it, it never rewrites the definition text, so string literals holding a schema name survive untouched.
   *
   * This must run after the functions were cloned above, since the copies have to exist to be bound to, and before
   * the triggers and views are created below, so nothing fires or depends on the columns while they are altered.
   * Every rendering below is a single statement, so the search_path cannot change midway through evaluating it.
   */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema);

  /* Plain column defaults. This is also what re-points a nextval() default at the sequence created for this schema:
   * a regclass literal prints schema-qualified only when the sequence is not visible on the path. Identity columns
   * stay clear of this by themselves, since an identity is not a default and has no pg_attrdef row.
   */
  SELECT coalesce(array_agg(
      format('ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
             dest_schema, cls.relname, att.attname, pg_catalog.pg_get_expr(def.adbin, def.adrelid, true))
      ORDER BY cls.relname, att.attnum), ARRAY[]::text[])
    INTO rebind_stmts_
    FROM pg_catalog.pg_attrdef def
    JOIN pg_catalog.pg_attribute att ON att.attrelid = def.adrelid AND att.attnum = def.adnum
    JOIN pg_catalog.pg_class cls ON cls.oid = def.adrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND att.attgenerated = '';

  /* Generated columns. SET EXPRESSION swaps in the re-rendered expression and rewrites the table to recompute the
   * stored values, which is harmless: the rows were copied above through the source schema's copy of the function,
   * whose body is identical to the copy made for this schema.
   *
   * Only stored columns are rebound. PostgreSQL 18 rejects user-defined functions in a virtual generation
   * expression, so a virtual column can only call built-ins out of pg_catalog, which are never cloned and so never
   * left pointing at the wrong schema.
   */
  SELECT rebind_stmts_ || coalesce(array_agg(
      format('ALTER TABLE %I.%I ALTER COLUMN %I SET EXPRESSION AS (%s)',
             dest_schema, cls.relname, att.attname, pg_catalog.pg_get_expr(def.adbin, def.adrelid, true))
      ORDER BY cls.relname, att.attnum), ARRAY[]::text[])
    INTO rebind_stmts_
    FROM pg_catalog.pg_attrdef def
    JOIN pg_catalog.pg_attribute att ON att.attrelid = def.adrelid AND att.attnum = def.adnum
    JOIN pg_catalog.pg_class cls ON cls.oid = def.adrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND att.attgenerated = 's';

  /* CHECK constraints have no ALTER ... SET form, so drop the copies LIKE made and add them back from the source's
   * definition. The copies are dropped by the name they actually carry on the destination and added back under the
   * name they have on the source, which keeps the clone's constraints named after the template's whatever LIKE
   * decided to call them. Restrict this to contype 'c': PostgreSQL 18 also keeps NOT NULL constraints in
   * pg_constraint, as contype 'n', and those must be left alone. pg_get_constraintdef() carries NOT VALID and
   * NO INHERIT along.
   */
  SELECT coalesce(array_agg(format('ALTER TABLE %I.%I DROP CONSTRAINT %I', dest_schema, cls.relname, con.conname)
      ORDER BY cls.relname, con.conname), ARRAY[]::text[])
    INTO rebind_drops_
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = dest_schema AND cls.relkind = 'r' AND con.contype = 'c';

  SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
      dest_schema, cls.relname, con.conname, pg_catalog.pg_get_constraintdef(con.oid, true))
      ORDER BY cls.relname, con.conname), ARRAY[]::text[])
    INTO rebind_adds_
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND con.contype = 'c';
  rebind_stmts_ := rebind_stmts_ || rebind_drops_ || rebind_adds_;

  /* Unique and exclusion constraints, for their names. LIKE brings the constraints themselves across but names them
   * by PostgreSQL's own rules, so a unique_together Django called <table>_<cols>_<hash>_uniq arrives as
   * <table>_<cols>_key and the shard stops agreeing with the template about what its constraints are called. The
   * index rebinding below cannot repair that: an index backing a constraint cannot be dropped on its own, so the
   * constraint has to be dropped and added back the way the CHECK constraints above are.
   *
   * This runs before the foreign keys are added, so nothing references these yet: dropping a unique constraint that
   * an FK had already been pointed at would fail. Restricted to contype 'u' and 'x'; primary keys are left alone,
   * since Django and PostgreSQL both name those <table>_pkey and they already match.
   */
  SELECT coalesce(array_agg(format('ALTER TABLE %I.%I DROP CONSTRAINT %I', dest_schema, cls.relname, con.conname)
      ORDER BY cls.relname, con.conname), ARRAY[]::text[])
    INTO rebind_drops_
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = dest_schema AND cls.relkind = 'r' AND con.contype IN ('u', 'x');

  SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
      dest_schema, cls.relname, con.conname, pg_catalog.pg_get_constraintdef(con.oid, true))
      ORDER BY cls.relname, con.conname), ARRAY[]::text[])
    INTO rebind_adds_
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND con.contype IN ('u', 'x');
  rebind_stmts_ := rebind_stmts_ || rebind_drops_ || rebind_adds_;

  /* Foreign keys. LIKE copies none at all, so add each of the source's from its own definition, rendered under the
   * source-only path like the CHECK constraints above: a parent in this schema prints unqualified and binds to the
   * destination's copy at execution, while a parent in another schema (public) stays qualified. The definition
   * carries composite column lists, MATCH, ON DELETE/ON UPDATE actions, deferrability and NOT VALID along, so the
   * clone gets exactly what the source declared.
   */
  SELECT coalesce(array_agg(format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
      dest_schema, cls.relname, con.conname, pg_catalog.pg_get_constraintdef(con.oid, true))
      ORDER BY cls.relname, con.conname), ARRAY[]::text[])
    INTO rebind_adds_
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND con.contype = 'f';
  rebind_stmts_ := rebind_stmts_ || rebind_adds_;

  /* Indexes that do not back a constraint. An index owned by a primary key, unique or exclusion constraint cannot
   * be dropped on its own, and a plain key index holds no expression to rebind anyway.
   *
   * LIKE names the indexes it creates by the default rules rather than after the originals, so the copies cannot be
   * addressed by the source's names: drop whatever non-constraint indexes the destination table ended up with, then
   * create the source's again from their own definitions. That restores the template's index names on the shard as
   * well. The pretty form of pg_get_indexdef() respects path visibility (the materialized view index cloning below
   * leans on the same property), and a recreated index lands in the destination schema because an index always
   * follows the schema of its table.
   */
  SELECT coalesce(array_agg(format('DROP INDEX %I.%I', dest_schema, idx_cls.relname)
      ORDER BY idx_cls.relname), ARRAY[]::text[])
    INTO rebind_drops_
    FROM pg_catalog.pg_index idx
    JOIN pg_catalog.pg_class idx_cls ON idx_cls.oid = idx.indexrelid
    JOIN pg_catalog.pg_class cls ON cls.oid = idx.indrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = dest_schema AND cls.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.conindid = idx.indexrelid);

  SELECT coalesce(array_agg(pg_catalog.pg_get_indexdef(idx.indexrelid, 0, true)
      ORDER BY idx_cls.relname), ARRAY[]::text[])
    INTO rebind_adds_
    FROM pg_catalog.pg_index idx
    JOIN pg_catalog.pg_class idx_cls ON idx_cls.oid = idx.indexrelid
    JOIN pg_catalog.pg_class cls ON cls.oid = idx.indrelid
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = source_schema AND cls.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.conindid = idx.indexrelid);
  rebind_stmts_ := rebind_stmts_ || rebind_drops_ || rebind_adds_;

  EXECUTE 'SET LOCAL search_path = ' || quote_ident(dest_schema) || ',public,pg_catalog';
  FOREACH rebind_stmt_ IN ARRAY rebind_stmts_ LOOP
    EXECUTE rebind_stmt_;
  END LOOP;

  /* Restore the path the surrounding phases run under. */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema) || ',public,pg_catalog';

  /* After cloning all tables, carry the position of every identity-backed sequence over from the source schema.
   * LIKE ... INCLUDING ALL recreates an identity column with a fresh sequence starting at 1, and identity sequences
   * do not appear in information_schema.sequences, so the value-carrying loop at the top never sees them. Pair the
   * sequences through their owning (table, column), whatever the column is called.
   */
  FOR ident_rec_ IN
    SELECT cls.relname::text AS table_name, att.attname::text AS column_name
      FROM pg_catalog.pg_attribute att
      JOIN pg_catalog.pg_class cls ON cls.oid = att.attrelid
      JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
      WHERE nsp.nspname = source_schema AND cls.relkind = 'r' AND att.attidentity <> '' AND NOT att.attisdropped
  LOOP
    src_seq_ := pg_get_serial_sequence(format('%I.%I', source_schema, ident_rec_.table_name),
      ident_rec_.column_name);
    dest_seq_ := pg_get_serial_sequence(format('%I.%I', dest_schema, ident_rec_.table_name),
      ident_rec_.column_name);
    IF src_seq_ IS NOT NULL AND dest_seq_ IS NOT NULL THEN
      EXECUTE format('SELECT last_value, is_called FROM %s', src_seq_) INTO last_val_, is_called_;
      PERFORM setval(dest_seq_, last_val_, is_called_);
    END IF;
  END LOOP;

  /* Clone all views and materialized views from the source schema to the destination schema. Named with its schema,
   * since the search_path here is the source schema's and both functions are installed on public.
   */
  PERFORM public.clone_schema_views(source_schema, dest_schema);

  /* Reset the search path. clone_schema_views narrows it to read definitions, and a SET LOCAL issued inside a
   * function is not guaranteed to survive its return, so set it here rather than rely on what it left behind.
   */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema) || ',public,pg_catalog';

  /* For all tables and views, clone their triggers. This runs after the views were created above, since a trigger needs
   * its relation to exist, and after the data copy above, so no trigger fires during the copy. (Note that INSTEAD OF
   * triggers are what make a non-auto-updatable view writable).
   *
   * Definitions are read with only the source schema on the search_path, the same principle as the expression
   * rebinding above: the trigger's table and a same-schema function print unqualified and bind to this schema's
   * copies when the statement runs with the destination schema first on the path, while functions from other
   * schemas (public, most notably) stay qualified. The definition text itself is never rewritten, so WHEN clauses
   * and argument string literals survive untouched.
   */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema);

  SELECT coalesce(array_agg(pg_get_triggerdef(tg.oid, true) ORDER BY cls.relname, tg.tgname), ARRAY[]::text[])
    INTO trigger_defs_
    FROM pg_catalog.pg_trigger tg
    JOIN pg_catalog.pg_class cls ON tg.tgrelid = cls.oid
    JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
    WHERE nsp.nspname = source_schema
      AND NOT tg.tgisinternal;  /* Exclude internal triggers (e.g., for foreign keys) */

  EXECUTE 'SET LOCAL search_path = ' || quote_ident(dest_schema) || ',public,pg_catalog';
  FOREACH trigger_def_ IN ARRAY trigger_defs_ LOOP
    EXECUTE trigger_def_;
  END LOOP;

  /* Restore the caller's search_path (see the note at the top). */
  PERFORM set_config('search_path', entry_search_path_, true);
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
"""

clone_views_function = """
CREATE OR REPLACE FUNCTION public.clone_schema_views(source_schema TEXT, dest_schema TEXT) RETURNS VOID AS
$VIEWS$
DECLARE
  view_names_ TEXT[];
  view_defs_ TEXT[];
  view_opts_ TEXT[];
  view_kinds_ TEXT[];
  view_populated_ BOOLEAN[];
  view_done_ BOOLEAN[];
  view_count_ INT;
  view_created_ INT;
  view_idx_ INT;
  view_progress_ BOOLEAN;
  view_errors_ TEXT;
  view_index_def_ TEXT;
  view_index_defs_ TEXT[];
  view_stmt_ TEXT;
  view_stmts_ TEXT[];
  view_default_columns_ TEXT[];
  view_defaults_ TEXT[];
  view_default_idx_ INT;
  adapted_view_def TEXT;
  entry_search_path_ TEXT;

BEGIN
  /* SET LOCAL survives function return until the transaction ends, so remember the caller's search_path;
   * move_sharded_models calls this function on its own inside an open transaction.
   */
  entry_search_path_ := current_setting('search_path');

  /* Recreate every view and materialized view of the source schema on the destination schema, rather than copy them,
   * to keep proper view semantics.
   *
   * Read the definitions with only the source schema on the search_path: pg_get_viewdef() and related functions
   * schema-qualify a reference when invisible on the current path, so same-schema references print unqualified while
   * references into other schemas print qualified. That is what rebinds a cloned view to the tables of its own schema.
   */
  EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema);

  SELECT
      array_agg(v.relname ORDER BY v.relname),
      array_agg(v.viewdef ORDER BY v.relname),
      array_agg(v.reloptions ORDER BY v.relname),
      array_agg(v.relkind ORDER BY v.relname),
      array_agg(v.relispopulated ORDER BY v.relname)
    INTO view_names_, view_defs_, view_opts_, view_kinds_, view_populated_
    FROM (
      SELECT
        c.relname::text AS relname,
        pg_catalog.pg_get_viewdef(c.oid, true) AS viewdef,
        coalesce(array_to_string(c.reloptions, ', '), '') AS reloptions,
        c.relkind::text AS relkind,
        c.relispopulated AS relispopulated
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = source_schema AND c.relkind IN ('v', 'm')
    ) AS v;

  view_count_ := coalesce(array_length(view_names_, 1), 0);

  IF view_count_ > 0 THEN
    EXECUTE 'SET LOCAL search_path = ' || quote_ident(dest_schema) || ',public,pg_catalog';

    /* Build every CREATE statement exactly once; the retry loop below may visit a view many times. */
    view_stmts_ := ARRAY[]::text[];
    FOR view_idx_ IN 1 .. view_count_ LOOP
      /* pg_get_viewdef() terminates its output with a semicolon. */
      adapted_view_def := regexp_replace(view_defs_[view_idx_], ';\\s*$', '');

      IF view_kinds_[view_idx_] = 'm' THEN
        view_stmt_ := 'CREATE MATERIALIZED VIEW ';
      ELSE
        view_stmt_ := 'CREATE VIEW ';
      END IF;

      view_stmt_ := view_stmt_ || quote_ident(dest_schema) || '.' || quote_ident(view_names_[view_idx_]);

      IF view_opts_[view_idx_] <> '' THEN
        view_stmt_ := view_stmt_ || ' WITH (' || view_opts_[view_idx_] || ')';
      END IF;

      view_stmt_ := view_stmt_ || ' AS ' || adapted_view_def;

      IF view_kinds_[view_idx_] = 'm' AND NOT view_populated_[view_idx_] THEN
        view_stmt_ := view_stmt_ || ' WITH NO DATA';
      END IF;

      view_stmts_ := view_stmts_ || view_stmt_;
    END LOOP;

    view_done_ := array_fill(false, ARRAY[view_count_]);
    view_created_ := 0;

    /* A view may select from another view, and a materialized view may select from a view, so the dependency order is
     * hard to determine. Rather than try to decude it, simply trial-and-error-retry until a full round creates nothing
     * new.
     */
    LOOP
      view_progress_ := false;
      view_errors_ := '';

      FOR view_idx_ IN 1 .. view_count_ LOOP
        CONTINUE WHEN view_done_[view_idx_];

        BEGIN
          EXECUTE view_stmts_[view_idx_];

          view_done_[view_idx_] := true;
          view_created_ := view_created_ + 1;
          view_progress_ := true;
        EXCEPTION
          WHEN OTHERS THEN
            /* If we create this view out of order and haven't fulfilled a dependency yet, this will error. Keep
             * every error of the round around, so that when a round only produces errors and no progress, each
             * genuine failure is reported rather than just whichever view happened to error last.
             */
            view_errors_ := view_errors_ || E'\n  ' || SQLERRM || ' -- while running: ' || view_stmts_[view_idx_];
        END;
      END LOOP;

      EXIT WHEN view_created_ = view_count_;

      IF NOT view_progress_ THEN
        RAISE EXCEPTION 'clone_schema could not create all views on schema %:%', dest_schema, view_errors_;
      END IF;
    END LOOP;

    FOR view_idx_ IN 1 .. view_count_ LOOP
      IF view_kinds_[view_idx_] = 'm' THEN
        /* CREATE MATERIALIZED VIEW copies no indexes, and without a unique index a materialized view cannot be
         * refreshed concurrently. Materialize the definitions with only the source schema visible (same principle
         * as the view definitions above), then execute them against the destination.
         */
        EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema);
        /* The pretty form respects path visibility, so use that instead of the plain form. */
        SELECT coalesce(array_agg(pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)), ARRAY[]::text[])
          INTO view_index_defs_
          FROM pg_catalog.pg_index i
          WHERE i.indrelid = format('%I.%I', source_schema, view_names_[view_idx_])::regclass;

        EXECUTE 'SET LOCAL search_path = ' || quote_ident(dest_schema) || ',public,pg_catalog';
        FOREACH view_index_def_ IN ARRAY view_index_defs_ LOOP
          EXECUTE view_index_def_;
        END LOOP;
      ELSE
        /* Column defaults set with ALTER VIEW ... SET DEFAULT are not part of the view definition, so restore those
         * manually to the cloned view.
         */
        EXECUTE 'SET LOCAL search_path = ' || quote_ident(source_schema);
        SELECT
          coalesce(array_agg(a.attname::text ORDER BY a.attnum), ARRAY[]::text[]),
          coalesce(array_agg(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) ORDER BY a.attnum), ARRAY[]::text[])
          INTO view_default_columns_, view_defaults_
          FROM pg_catalog.pg_attrdef d
          JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
          WHERE d.adrelid = format('%I.%I', source_schema, view_names_[view_idx_])::regclass;

        EXECUTE 'SET LOCAL search_path = ' || quote_ident(dest_schema) || ',public,pg_catalog';
        FOR view_default_idx_ IN 1 .. coalesce(array_length(view_default_columns_, 1), 0) LOOP
          EXECUTE 'ALTER VIEW ' || quote_ident(dest_schema) || '.' || quote_ident(view_names_[view_idx_])
            || ' ALTER COLUMN ' || quote_ident(view_default_columns_[view_default_idx_]) || ' SET DEFAULT '
            || view_defaults_[view_default_idx_];
        END LOOP;
      END IF;
    END LOOP;

  END IF;

  /* Restore the caller's search_path (the definition load above narrowed it even if there were no views to create). */
  PERFORM set_config('search_path', entry_search_path_, true);
END;
$VIEWS$
LANGUAGE plpgsql VOLATILE;
"""

# Both functions are installed together: clone_schema calls clone_schema_views, and move_sharded_models calls the
# latter on its own to bring the template's views onto a schema whose tables arrived some other way.
clone_function = clone_views_function + clone_schema_function

PUBLIC_SCHEMA_NAME = 'public'


def get_validated_schema_name(schema_name, is_template=False):
    from djanquiltdb.utils import get_template_name  # Prevent cyclic imports

    if not isinstance(schema_name, str):
        raise ValueError("Schema name '{}' needs to be a string".format(schema_name))

    if not re.match(r'^[A-Za-z][0-9A-Za-z_]*$', schema_name):
        raise ValueError(
            "Schema name '{}' contains illegal characters and/or does not start with a letter".format(schema_name)
        )

    if not is_template and schema_name == get_template_name():
        raise ValueError(
            "Schema name '{}' cannot be the same as the template name '{}' ".format(schema_name, get_template_name())
        )
    if schema_name in [PUBLIC_SCHEMA_NAME, 'information_schema', 'default']:
        raise ValueError("Schema name '{}' is not allowed ".format(schema_name))

    if schema_name.startswith('pg_'):
        raise ValueError(
            "Schema name '{}' is not allowed to mimic PostgreSQL native schema names (starting with 'pg_')".format(
                schema_name
            )
        )

    return schema_name


def get_database_creation_class():
    return import_string(
        settings.QUILT_DB.get('DATABASE_CREATION_CLASS', 'djanquiltdb.postgresql_backend.creation.DatabaseCreation')
    )


class DatabaseWrapper(BaseDatabaseWrapper):
    """
    Adds the capability to manipulate the search_path using set_schema and set_schema_to_public
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Replace the default introspection with a patched version of
        # the DatabaseIntrospection that only returns the table list
        # for the currently selected schema.
        self.introspection = DatabaseSchemaIntrospection(self)

        # Give the library user the possibility to overwrite the database creation class. This allows them to adjust the
        # creation of the test database, to make a default shard for example.
        self.creation = get_database_creation_class()(self)

        self.current_search_paths = [PUBLIC_SCHEMA_NAME]

        self.schema_name = PUBLIC_SCHEMA_NAME
        self.include_public_schema = True

        # Django < 2.0 require this attribute set, in our case it should be just a noop.
        if hasattr(self, '_start_transaction_under_autocommit'):
            self._start_transaction_under_autocommit = lambda x: None

    def __str__(self):
        return self.alias

    def close(self):
        self.current_search_paths = [PUBLIC_SCHEMA_NAME]
        super().close()

    def rollback(self):
        try:
            super().rollback()
        finally:
            self.current_search_paths = None

    def commit(self):
        # Invalidate current_search_paths so the next _cursor() call re-issues SET search_path. Required for PgBouncer
        # transaction pooling (the next transaction may be handed a different physical connection with no search_path
        # set) and for direct connections (SET search_path is session-level, not transactional, so we cannot pretend the
        # session was reset just because the transaction ended). None as sentinel never compares equal to a list, so the
        # early-exit in _cursor() always falls through.
        try:
            super().commit()
        finally:
            self.current_search_paths = None

    def get_schema(self):
        return self.schema_name

    def is_public_schema(self):
        return self.schema_name == PUBLIC_SCHEMA_NAME

    def get_ps_schema(self, schema_name, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s);', [schema_name])
        if cursor.fetchall()[0][0]:
            return schema_name

    def get_all_pg_schemas(self, _cursor=None):
        return self.introspection.get_schema_names(_cursor or self.cursor())

    def get_all_table_headers(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE';",
            [schema],
        )
        return [x[0] for x in cursor.fetchall()]  # We get a list of single tuples

    def get_copyable_column_names(self, table_name, schema_name=None, _cursor=None):
        """
        Return writable column names for a table (i.e. excluding generated columns) in declaration order.

        Must be kept in sync with the row-copy block in clone_schema's plpgsql body.
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            """
            SELECT att.attname::text
            FROM pg_catalog.pg_attribute att
            JOIN pg_catalog.pg_class cls ON att.attrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s
              AND cls.relname = %s
              AND att.attnum > 0
              AND NOT att.attisdropped
              AND att.attgenerated = ''
            ORDER BY att.attnum
        """,
            [schema, table_name],
        )
        return [x[0] for x in cursor.fetchall()]  # We get a list of single tuples

    def get_copyable_column_names_by_table(self, schema_name=None, _cursor=None):
        """
        Writable column names for every table in the schema. Same rules as get_copyable_column_names.

        Must be kept in sync with the row-copy block in clone_schema's plpgsql body.
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            """
            SELECT cls.relname::text, array_agg(att.attname::text ORDER BY att.attnum)
            FROM pg_catalog.pg_attribute att
            JOIN pg_catalog.pg_class cls ON att.attrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s
              AND cls.relkind IN ('r', 'p')
              AND att.attnum > 0
              AND NOT att.attisdropped
              AND att.attgenerated = ''
            GROUP BY cls.relname
        """,
            [schema],
        )
        return {table: columns for table, columns in cursor.fetchall()}

    def get_populated_materialized_views(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            """
            SELECT cls.relname::text
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s AND cls.relkind = 'm' AND cls.relispopulated
        """,
            [schema],
        )
        return {name for (name,) in cursor.fetchall()}

    def get_materialized_views_in_dependency_order(self, schema_name=None, _cursor=None):
        """
        Return the schema's materialized views, each listed after every materialized view it reads.

        The order is worked out over the whole view graph, plain views included, because a materialized view may read
        a plain view. Only the materialized ends of such a chain can be refreshed, but leaving the plain view out of the
        graph loses the edge between them and lets the dependent refresh first, against rows that are still stale.
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        relkinds = dict(self.get_all_views(schema_name=schema, _cursor=cursor))
        cursor.execute(
            """
            SELECT DISTINCT dependent.relname::text, referenced.relname::text
            FROM pg_catalog.pg_depend dep
            JOIN pg_catalog.pg_rewrite rew ON dep.objid = rew.oid
            JOIN pg_catalog.pg_class dependent ON rew.ev_class = dependent.oid
            JOIN pg_catalog.pg_namespace nsp ON dependent.relnamespace = nsp.oid
            JOIN pg_catalog.pg_class referenced ON dep.refobjid = referenced.oid
            JOIN pg_catalog.pg_namespace refnsp ON referenced.relnamespace = refnsp.oid
            WHERE nsp.nspname = %s AND refnsp.nspname = %s
              AND dependent.relkind IN ('v', 'm') AND referenced.relkind IN ('v', 'm')
              AND dependent.oid <> referenced.oid
        """,
            [schema, schema],
        )
        depends_on = {}
        for dependent, referenced in cursor.fetchall():
            depends_on.setdefault(dependent, set()).add(referenced)

        ordered = []
        # Both sides of the query above stay inside the one schema. A refresh is per-schema, so a view reaching
        # through another schema is not something this ordering could act on anyway.
        remaining = set(relkinds)
        while remaining:
            ready = sorted(name for name in remaining if not (depends_on.get(name, set()) & remaining))
            if not ready:
                # Views cannot form a dependency cycle; guard against an infinite loop anyway.
                ordered.extend(sorted(remaining))
                break
            ordered.extend(ready)
            remaining.difference_update(ready)
        # The plain views were only in the graph to carry the edges between materialized ones through.
        return [name for name in ordered if relkinds[name] == 'm']

    def refresh_materialized_views(self, names=None, schema_name=None, _cursor=None):
        """
        Repopulate the schema's materialized views, each after the ones it reads.

        `names` limits the refresh to the views named, which is how a caller keeps a view unpopulated if it was left
        that way on purpose. The default is every populated view of the schema.
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        if names is None:
            names = self.get_populated_materialized_views(schema_name=schema, _cursor=cursor)

        quote_name = self.ops.quote_name
        for name in self.get_materialized_views_in_dependency_order(schema_name=schema, _cursor=cursor):
            if name in names:
                # Schema-qualified: a shard's search path covers the public schema too, so an unqualified refresh
                # could reach whichever copy the path finds first rather than the one asked for.
                cursor.execute(
                    'REFRESH MATERIALIZED VIEW "{schema}".{name}'.format(schema=schema, name=quote_name(name))  # nosec
                )

    def get_all_table_sequences(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            """
            SELECT cls.relname::text
            FROM pg_catalog.pg_sequence seq
            JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s
        """,
            [schema],
        )
        return [x[0] for x in cursor.fetchall()]  # We get a list of single tuples

    def get_all_views(self, schema_name=None, _cursor=None):
        """
        Return a (name, relkind) tuple for every view ('v') and materialized view ('m') on the given schema.
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        cursor.execute(
            """
            SELECT cls.relname::text, cls.relkind::text
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE nsp.nspname = %s
              AND cls.relkind IN ('v', 'm')
        """,
            [schema],
        )
        return cursor.fetchall()

    def truncate_all_tables(self, schema_name=None, _cursor=None):
        cursor = _cursor or self.cursor()
        table_headers = self.get_all_table_headers(schema_name, cursor)
        cursor.execute('TRUNCATE ONLY {} CASCADE;'.format(', '.join('"{}"'.format(header) for header in table_headers)))

    def flush_schema(self, schema_name=None, _cursor=None):
        """
        Drops all tables on the given schema
        """
        cursor = _cursor or self.cursor()
        schema = schema_name or self.get_schema()
        # Get all sequences first, before dropping tables
        sequences = self.get_all_table_sequences(schema_name=schema)
        # Drop views before the tables they read from. Dropping one view cascades to anything built on top of it, so
        # the next one in the list may already be gone by the time we get to it.
        for view, relkind in self.get_all_views(schema_name=schema):
            statement = 'DROP MATERIALIZED VIEW IF EXISTS' if relkind == 'm' else 'DROP VIEW IF EXISTS'
            cursor.execute(
                '{statement} "{schema}"."{view}" CASCADE'.format(statement=statement, schema=schema, view=view)
            )
        # Drop tables with CASCADE (this will also drop sequences owned by tables)
        # Use schema-qualified table names to ensure we drop from the correct schema
        for table in self.get_all_table_headers(schema_name=schema):
            cursor.execute('DROP TABLE "{schema}"."{table}" CASCADE'.format(schema=schema, table=table))
        # Drop any remaining sequences that weren't owned by tables
        # (DROP TABLE CASCADE drops sequences owned by tables, but sequences can exist independently)
        for sequence in sequences:
            cursor.execute(
                'DROP SEQUENCE IF EXISTS "{schema}"."{sequence}" CASCADE'.format(schema=schema, sequence=sequence)
            )

    def get_schema_for_model(self, model, _cursor=None):
        """
        Returns the schema the given model lives on.
        """
        cursor = _cursor or self.cursor()
        # Note: read from pg_class rather than information_schema.tables: a model's db_table may be a view, and
        # materialized views do not appear in information_schema.tables at all.
        cursor.execute(
            'SELECT n.nspname FROM pg_catalog.pg_class c'
            ' JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace'
            " WHERE c.relname = %s AND c.relkind IN ('r', 'p', 'v', 'm');",
            [model._meta.db_table],
        )
        return cursor.fetchall()

    def get_schema_for_sequence(self, sequence_name, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute(
            """
            SELECT nspname::text
            FROM pg_catalog.pg_sequence seq
            JOIN pg_catalog.pg_class cls ON seq.seqrelid = cls.oid
            JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
            WHERE cls.relname = %s
        """,
            [sequence_name],
        )
        return cursor.fetchall()

    def create_schema(self, schema_name, is_template=False):
        schema_name = get_validated_schema_name(schema_name, is_template=is_template)
        cursor = self.cursor()
        cursor.execute(
            'CREATE SCHEMA IF NOT EXISTS "{}";'.format(schema_name)
        )  # Params cannot be used for schema names

    def delete_schema(self, schema_name, is_template=False):
        schema_name = get_validated_schema_name(schema_name, is_template=is_template)
        cursor = self.cursor()
        cursor.execute('DROP SCHEMA "{}" CASCADE;'.format(schema_name))  # Params cannot be used for schema names

    def clone_schema(self, from_schema, to_schema):
        cursor = self.cursor()
        if not self.get_ps_schema(from_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(from_schema, self))
        if not self.get_ps_schema(to_schema, cursor):
            raise ValueError("Schema '{}' does not exist on node '{}'.".format(to_schema, self))

        self.set_clone_function()

        cursor.execute('SELECT clone_schema(%s, %s);', [from_schema, to_schema])

    def set_clone_function(self, _cursor=None):
        cursor = _cursor or self.cursor()
        cursor.execute(clone_function)

    def reset_sequence(self, model_list, _cursor=None):
        from django.db import models

        cursor = _cursor or self.cursor()
        qn = self.ops.quote_name
        auto_columns = []
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    auto_columns.append((model._meta.db_table, f.column))
                    break  # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                # Django < 2.0
                remote_field = 'rel' if hasattr(f, 'rel') else 'remote_field'
                if not getattr(f, remote_field).through:
                    auto_columns.append((f.m2m_db_table(), 'id'))
        if not auto_columns:
            return

        # Look up each column's sequence through the column itself, since a table that was renamed keeps the sequence
        # it was created with. The column's sequence is the one its default takes values from, which is the one inserts
        # use. A column without such a default uses the sequence it owns, which is how an identity column is backed. If
        # the default takes values from several sequences, the one whose name sorts first is used. The sequence may be
        # in any schema. If a column has no sequence, or the schema has no such table or column, a ValueError is raised
        # before any sequence is moved.
        cursor.execute(
            'SELECT coalesce(('
            'SELECT dep.refobjid::regclass FROM pg_catalog.pg_attrdef def'
            ' JOIN pg_catalog.pg_attribute att ON att.attrelid = def.adrelid AND att.attnum = def.adnum'
            " JOIN pg_catalog.pg_depend dep ON dep.classid = 'pg_catalog.pg_attrdef'::regclass AND dep.objid = def.oid"
            " JOIN pg_catalog.pg_class seq_cls ON seq_cls.oid = dep.refobjid AND seq_cls.relkind = 'S'"
            " WHERE dep.refclassid = 'pg_catalog.pg_class'::regclass"
            ' AND def.adrelid = pg_catalog.to_regclass(pk_columns.table_name)'
            ' AND att.attname = pk_columns.column_name'
            ' ORDER BY seq_cls.relname LIMIT 1),'
            ' CASE WHEN EXISTS ('
            'SELECT 1 FROM pg_catalog.pg_attribute att'
            ' WHERE att.attrelid = pg_catalog.to_regclass(pk_columns.table_name) AND att.attname = pk_columns.column_name'
            ' AND NOT att.attisdropped)'
            ' THEN pg_get_serial_sequence(pk_columns.table_name, pk_columns.column_name)::regclass END)::oid'
            ' FROM unnest(%s::text[], %s::text[]) WITH ORDINALITY AS pk_columns(table_name, column_name, position)'
            ' ORDER BY pk_columns.position',
            [[qn(table_name) for table_name, _ in auto_columns], [column for _, column in auto_columns]],
        )
        sequence_oids = [row[0] for row in cursor.fetchall()]
        columns_without_sequence = [
            '{}.{}'.format(table_name, column)
            for (table_name, column), sequence_oid in zip(auto_columns, sequence_oids)
            if sequence_oid is None
        ]
        if columns_without_sequence:
            raise ValueError('The column(s) {} have no sequence.'.format(', '.join(columns_without_sequence)))

        # Move each sequence to at least the max pk value, or 1 if there are no records. A sequence that is already
        # past that value keeps its own. Set the `is_called` property (the third argument to `setval`) to true when the
        # value is in use, otherwise set it to false.
        statement_template = (
            'SELECT setval({s}, GREATEST(coalesce(max({f}), 1), coalesce(pg_sequence_last_value({s}), 1)),'
            ' max({f}) IS NOT null OR pg_sequence_last_value({s}) IS NOT null) FROM {qnm}'
        )
        statements = [
            statement_template.format(s='{:d}::regclass'.format(sequence_oid), f=qn(column), qnm=qn(table_name))  # nosec
            for (table_name, column), sequence_oid in zip(auto_columns, sequence_oids)
        ]
        cursor.execute(';\n'.join(statements))

    def make_debug_cursor(self, cursor, skip_lock=False):
        """
        Creates a cursor that logs all queries in self.queries_log, and that can set advisory locks as well.
        """
        return CursorDebugWrapper(cursor, self, lock=getattr(self, 'lock_on_execute', False) and not skip_lock)

    def make_cursor(self, cursor, skip_lock=False):
        """
        Creates a cursor without debug logging, and that can set advisory locks as well.
        """
        return CursorWrapper(cursor, self, lock=getattr(self, 'lock_on_execute', False) and not skip_lock)

    def acquire_advisory_lock(self, key, shared=True, xact=False, _cursor=None):
        """
        Set a shared or exclusive advisory lock on a given key, session-scoped or transaction-scoped.
        """
        cursor = _cursor or self.cursor()
        cursor.acquire_advisory_lock(key, shared=shared, xact=xact)

    def release_advisory_lock(self, key, shared=True, _cursor=None):
        """
        Release a shared or exclusive advisory lock on a given key.
        """
        cursor = _cursor or self.cursor()
        cursor.release_advisory_lock(key, shared=shared)

    def cursor(self):
        """
        Note that this is backported from Django 1.11 to have the same behaviour between Django 1.8 to Django 1.11.
        """
        return self._cursor()

    def _cursor(self, name=None):
        """Database cursor to write whatever we want.

        Typically used for migrations, this function will check
        to see if SCHEMA_NAME is set or not. If it is, then it
        will create it if it doesn't yet exist. Finally, it will
        point to that schema.

        Check for a connection and see if it is usable. If not: close the connection and the Super() class will
        automatically reconnect in its _cursor() function.
        """
        if self.connection is not None and self.errors_occurred:
            if not self.is_usable():
                logger.warning('Database connection is unusable. Reconnecting and continuing.')
                self.close()

        cursor = self._get_cursor(name=name)

        if self.include_public_schema and self.schema_name != PUBLIC_SCHEMA_NAME:
            search_paths = [self.schema_name, PUBLIC_SCHEMA_NAME]
        else:
            search_paths = [self.schema_name]

        # No need to set search paths for operations without a database,
        # or when there are no changes to the selected schemas.
        if self.alias == NO_DB_ALIAS or self.current_search_paths == search_paths:
            return cursor

        # Use unnamed cursors for schema operations - named cursors require SELECT statements
        # and cannot execute SET commands like SET search_path
        with self._get_cursor(name=None, skip_lock=True) as cursor_for_get_ps_schema:
            if self.schema_name != PUBLIC_SCHEMA_NAME and not self.get_ps_schema(
                self.schema_name, cursor_for_get_ps_schema
            ):
                raise IntegrityError("Schema '{}' does not exist.".format(self.schema_name))

        with self._get_cursor(name=None, skip_lock=True) as cursor_for_search_path:
            # In the event that an error already happened in this transaction and we are going
            # to rollback we should just ignore database error when setting the search_path
            # if the next instruction is not a rollback it will just fail also, so
            # we do not have to worry that it's not the good one
            try:
                identifiers = [sql.Identifier(x) for x in search_paths]
                sql_ = sql.SQL('SET search_path = {}').format(sql.SQL(', ').join(identifiers))
                cursor_for_search_path.execute(sql_)
                logger.debug(str(sql_))
            except DatabaseError, InternalError:
                logger.warning('Something went wrong with setting the search path.', exc_info=True)
            else:
                self.current_search_paths = search_paths

        return cursor

    def _get_cursor(self, name=None, skip_lock=False):
        """
        Copied from Django 1.11's _cursor() method with the addition of `skip_lock`. Note that this is different from
        the _cursor() method from previous versions. For this library to work easily with multiple Django versions, we
        backported this from Django 1.11.
        """
        self.ensure_connection()
        with self.wrap_database_errors:
            cursor = self.create_cursor(name)
            return self._prepare_cursor(cursor, skip_lock=skip_lock)

    def _prepare_cursor(self, cursor, skip_lock=False):
        """
        Validate the connection is usable and perform database cursor wrapping. Copied from Django 1.11, but with an
        addition of `skip_lock`, which will return a cursor that doesn't do locking if `skip_lock` is True.
        """
        self.validate_thread_sharing()
        if self.queries_logged:
            wrapped_cursor = self.make_debug_cursor(cursor, skip_lock=skip_lock)
        else:
            wrapped_cursor = self.make_cursor(cursor, skip_lock=skip_lock)
        return wrapped_cursor


class ShardDatabaseWrapper(DatabaseWrapper):
    """
    Wrapper around DatabaseWrapper that handles shard routing. This class shares the connection of the database wrapper
    it wraps (from now one called the main connection). This ensures that we are not making a new connection to the
    database each time we switch to a certain schema name. In order to do this, we route all calls to the properties
    defined in _PROXY_FIELDS to the main connection instance (which are the same fields as in the constructor of
    DjangoBaseDatabaseWrapper, excluding `alias`, but including `current_search_paths`).

    Note that this class should not be used for connections to the public schema. You can use the main connection for
    that one.
    """

    _PROXY_FIELDS = (
        'connection',
        'settings_dict',
        'queries_log',
        'force_debug_cursor',
        'autocommit',
        'in_atomic_block',
        'atomic_blocks',
        'savepoint_state',
        'savepoint_ids',
        'commit_on_exit',
        'needs_rollback',
        'close_at',
        'closed_in_transaction',
        'errors_occurred',
        '_thread_ident',
        'current_search_paths',
        'run_on_commit',
        'run_commit_hooks_on_set_autocommit_on',
        'rollback_exc',
        'health_check_enabled',
        'health_check_done',
        'execute_wrappers',
    )

    _present_shard_options_as_alias = False

    def __init__(self, main_connection, options):
        self._main_connection = main_connection
        self.shard_options = options

        # We proxy the fields specified in _PROXY_FIELDS to the main connection. Because we call the super().__init__
        # here, that means that we would reset the values of the fields we proxy. We don't want that here, so we keep
        # track whether we are in the initialization state and only set the proxy values outside of the __init__ method.
        self._initialized = False
        super().__init__(
            settings_dict=main_connection.settings_dict,
            alias=main_connection.alias,
        )
        self._initialized = True

        if self.shard_options.schema_name == PUBLIC_SCHEMA_NAME:
            raise ValueError('Connection to the public schema should be handled by the default DatabaseWrapper.')

        self.schema_name = options.schema_name
        self.include_public_schema = options.kwargs.get('include_public', True)

        # Determine whether we need to set an advisory lock or not. If use_shard on the options is True, this means that
        # we activated this connection in a context manager, meaning that we already activated the lock and we don't
        # have to do that in the cursor's execute method.
        self.lock_on_execute = bool(options.lock and not options.use_shard and options.lock_keys)

    @property
    def alias(self):
        # See postgresql_backend.operations.patch_in_lookup() for information on this switch
        if self._present_shard_options_as_alias:
            return self.shard_options

        return '{}|{}'.format(self._main_connection.alias, self.schema_name)

    @alias.setter
    def alias(self, value):
        if value != self._main_connection.alias:
            raise ValueError('The alias is managed by the main connection and cannot be changed.')

    def __getattribute__(self, item):
        if item in ShardDatabaseWrapper._PROXY_FIELDS:
            return getattr(self._main_connection, item)
        return super().__getattribute__(item)

    def __setattr__(self, key, value):
        # We don’t want to reset the attributes on the main connection when initializing this class instance, hence we
        # check on the value of self._initialized here.
        if key in ShardDatabaseWrapper._PROXY_FIELDS and self._initialized:
            return setattr(self._main_connection, key, value)
        return super().__setattr__(key, value)

    def acquire_locks(self, shared=True):
        # Inside a transaction the locks are transaction-scoped, so the rollback a failure inside the context
        # forces releases them; see LockCursorWrapperMixin._lock for the rationale.
        xact = self.in_atomic_block
        for key in self.shard_options.lock_keys:
            self.acquire_advisory_lock(key, shared=shared, xact=xact)

    def release_locks(self, shared=True):
        for key in self.shard_options.lock_keys:
            self.release_advisory_lock(key, shared=shared)
