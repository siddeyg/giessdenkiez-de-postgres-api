-- ---------------------------------------------------------------------------
-- gdk_stats_cache: single-row table holding the pre-computed stats payload.
-- The Edge Function reads this on every request instead of running 10 heavy
-- queries live. The cache is refreshed at most once per day (lazy: the first
-- stale request of the day triggers a recompute).
-- ---------------------------------------------------------------------------

CREATE TABLE public.gdk_stats_cache (
  -- Single-row constraint: only id=1 is allowed.
  id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  -- Full stats payload as returned by the Edge Function (minus numPumps,
  -- which comes from an external URL and is merged in at request time).
  payload jsonb NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now()
);

-- Only the service role (used by the Edge Function) can read or write.
ALTER TABLE public.gdk_stats_cache ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- refresh_gdk_stats_cache(): recomputes all DB-derived stats and upserts the
-- result into gdk_stats_cache. Called by the Edge Function on a cache miss
-- (first request of the day). numPumps is NOT included here — it is fetched
-- live from Supabase Storage and merged into the response by the Edge Function.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_gdk_stats_cache()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_waterings_count bigint;
  v_users_count     bigint;
  v_adoptions       jsonb;
  v_monthly_waterings jsonb;
  v_waterings       jsonb;
  v_monthly_weather jsonb;
  v_species         jsonb;
  v_payload         jsonb;
BEGIN
  -- Count waterings this calendar year
  SELECT COUNT(*) INTO v_waterings_count
  FROM trees_watered
  WHERE timestamp > date_trunc('year', CURRENT_DATE);

  -- Count registered user profiles
  SELECT COUNT(*) INTO v_users_count FROM profiles;

  -- Adoption totals
  SELECT jsonb_build_object(
    'count',           total_adoptions,
    'veryThirstyCount', very_thirsty_adoptions
  ) INTO v_adoptions
  FROM calculate_adoptions();

  -- Monthly watering aggregates
  SELECT jsonb_agg(jsonb_build_object(
    'month',                      month,
    'wateringCount',              watering_count,
    'totalSum',                   total_sum,
    'averageAmountPerWatering',   avg_amount_per_watering
  )) INTO v_monthly_waterings
  FROM calculate_avg_waterings_per_month();

  -- Individual waterings with lat/lng (this year only)
  SELECT jsonb_agg(jsonb_build_object(
    'id',        id,
    'lat',       lat,
    'lng',       lng,
    'amount',    amount,
    'timestamp', timestamp
  )) INTO v_waterings
  FROM get_waterings_with_location();

  -- Monthly weather (avg temperature + total rainfall)
  SELECT jsonb_agg(jsonb_build_object(
    'month',                    month,
    'averageTemperatureCelsius', avg_temperature_celsius,
    'totalRainfallLiters',      total_rainfall_liters
  )) INTO v_monthly_weather
  FROM get_monthly_weather();

  -- Top 20 tree species by count (from materialized view)
  SELECT jsonb_agg(jsonb_build_object(
    'speciesName', gattung_deutsch,
    'percentage',  percentage
  )) INTO v_species
  FROM most_frequent_tree_species;

  -- Assemble full payload
  v_payload := jsonb_build_object(
    'numTrees',               (SELECT count FROM trees_count),
    'numActiveUsers',         v_users_count,
    'numWateringsThisYear',   v_waterings_count,
    'treeAdoptions',          v_adoptions,
    'monthlyWaterings',       COALESCE(v_monthly_waterings, '[]'::jsonb),
    'waterings',              COALESCE(v_waterings,          '[]'::jsonb),
    'monthlyWeather',         COALESCE(v_monthly_weather,    '[]'::jsonb),
    'mostFrequentTreeSpecies', COALESCE(v_species,           '[]'::jsonb),
    'totalTreeSpeciesCount',  (SELECT count FROM total_tree_species_count)
  );

  INSERT INTO public.gdk_stats_cache (id, payload, computed_at)
  VALUES (1, v_payload, now())
  ON CONFLICT (id) DO UPDATE
    SET payload     = EXCLUDED.payload,
        computed_at = EXCLUDED.computed_at;
END;
$$;

GRANT EXECUTE ON FUNCTION public.refresh_gdk_stats_cache() TO service_role;
