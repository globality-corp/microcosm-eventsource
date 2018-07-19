/*
 * array_sort_unique(ANYARRAY)
 *
 * Return sorted and unique array
 */
CREATE OR REPLACE FUNCTION array_sort_unique (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT DISTINCT unnest($1) ORDER BY 1)
$$;
