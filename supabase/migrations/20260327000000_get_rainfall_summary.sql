-- get_rainfall_summary(days_back int)
--
-- Returns city-wide rainfall statistics aggregated from RADOLAN data
-- for the last N days. Values are averaged across all RADOLAN grid cells
-- covering the city area.
--
-- radolan_data.value is in 0.1 mm units → dividing by 10 gives mm.
-- radolan_data is protected by RLS; SECURITY DEFINER bypasses it.
--
-- Usage (anon key, no auth required):
--   SELECT * FROM get_rainfall_summary(7);   -- last 7 days
--   SELECT * FROM get_rainfall_summary(1);   -- yesterday
--   SELECT * FROM get_rainfall_summary(30);  -- last 30 days

CREATE OR REPLACE FUNCTION get_rainfall_summary(days_back int DEFAULT 7)
RETURNS TABLE (
    from_date     timestamp,
    to_date       timestamp,
    avg_mm        numeric,
    min_mm        numeric,
    max_mm        numeric,
    cell_count    bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, extensions
AS $$
    WITH cell_totals AS (
        SELECT
            geom_id,
            SUM(value)::numeric AS total_01mm
        FROM radolan_data
        WHERE measured_at > NOW() - (days_back || ' days')::interval
        GROUP BY geom_id
    ),
    date_range AS (
        SELECT
            MIN(measured_at) AS from_date,
            MAX(measured_at) AS to_date
        FROM radolan_data
        WHERE measured_at > NOW() - (days_back || ' days')::interval
    )
    SELECT
        date_range.from_date,
        date_range.to_date,
        ROUND(AVG(total_01mm) / 10.0, 1) AS avg_mm,
        ROUND(MIN(total_01mm) / 10.0, 1) AS min_mm,
        ROUND(MAX(total_01mm) / 10.0, 1) AS max_mm,
        COUNT(*)                          AS cell_count
    FROM cell_totals, date_range
    GROUP BY date_range.from_date, date_range.to_date;
$$;
