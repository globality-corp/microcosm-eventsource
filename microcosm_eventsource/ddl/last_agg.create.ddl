CREATE AGGREGATE last_agg (anyelement) (
      SFUNC = last_agg_sfunc,
      STYPE = anyelement
);
