"""
Raw DDL shared between test modules.
"""

#: An immutable function suitable for a stored generation expression, created in the public schema so every shard's
#: search path resolves it.
CREATE_ALLUPPERCASE = """
    CREATE FUNCTION public.alluppercase(input TEXT) RETURNS TEXT
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
        BEGIN
            RETURN UPPER(input);
        END;
    $$
"""

#: CASCADE, because a test case may have stacked a generated column on top.
DROP_ALLUPPERCASE = 'DROP FUNCTION IF EXISTS public.alluppercase(TEXT) CASCADE'
