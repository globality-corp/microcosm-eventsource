CREATE OR REPLACE FUNCTION last_agg_sfunc (state anyelement, value anyelement)
       RETURNS anyelement
       LANGUAGE SQL
       IMMUTABLE
AS $$
  SELECT coalesce(value, state);
$$;
