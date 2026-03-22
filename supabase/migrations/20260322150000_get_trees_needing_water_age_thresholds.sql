-- Update get_trees_needing_water: use age-based drought thresholds
-- matching the existing GDK water-needs model (use-tree-water-needs-data.tsx):
--   Baby  (≤ 5y):  < 100  (10mm)
--   Junior (6-10y): < 200 (20mm)
--   Senior (> 10y): < 300 (30mm)
--   Unknown:        < 200 (20mm)

CREATE OR REPLACE FUNCTION get_trees_needing_water(
  p_lat        float,
  p_lng        float,
  radius_m     int     DEFAULT 550,
  max_results  int     DEFAULT 5
)
RETURNS TABLE (
  id           text,
  lat          float,
  lng          float,
  radolan_sum  int,
  pflanzjahr   int,
  art_dtsch    text,
  distance_m   float,
  last_watered date
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, extensions
AS $$
  SELECT
    t.id,
    t.lat::float,
    t.lng::float,
    t.radolan_sum,
    t.pflanzjahr,
    t.art_dtsch,
    ROUND(ST_Distance(
      t.geom::geography,
      ST_MakePoint(p_lng, p_lat)::geography
    ))::float AS distance_m,
    MAX(tw.timestamp)::date AS last_watered
  FROM trees t
  LEFT JOIN trees_watered tw
    ON tw.tree_id = t.id
    AND tw.timestamp > NOW() - INTERVAL '30 days'
  WHERE
    t.geom IS NOT NULL
    AND t.lat IS NOT NULL
    AND t.lng IS NOT NULL
    AND ST_DWithin(
      t.geom::geography,
      ST_MakePoint(p_lng, p_lat)::geography,
      radius_m
    )
    AND t.radolan_sum IS NOT NULL
    AND t.radolan_sum < CASE
        WHEN t.pflanzjahr IS NULL                              THEN 200
        WHEN (EXTRACT(YEAR FROM NOW()) - t.pflanzjahr) <= 5   THEN 100
        WHEN (EXTRACT(YEAR FROM NOW()) - t.pflanzjahr) <= 10  THEN 200
        ELSE                                                       300
      END
  GROUP BY
    t.id, t.lat, t.lng, t.radolan_sum, t.pflanzjahr, t.art_dtsch, distance_m
  ORDER BY
    -- Lower rain = more urgent. Young trees weighted higher.
    (t.radolan_sum * 0.5)
    + CASE
        WHEN t.pflanzjahr IS NULL                              THEN 100
        WHEN (EXTRACT(YEAR FROM NOW()) - t.pflanzjahr) <= 5   THEN 0
        WHEN (EXTRACT(YEAR FROM NOW()) - t.pflanzjahr) <= 10  THEN 40
        ELSE 150
      END
    + (ST_Distance(
        t.geom::geography,
        ST_MakePoint(p_lng, p_lat)::geography
      ) * 0.05)
  LIMIT max_results;
$$;
