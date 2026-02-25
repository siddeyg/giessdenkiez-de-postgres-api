-- Returns the adoption count for a single tree.
-- Replaces the pattern of calling get_watered_and_adopted() (returns ALL trees)
-- and then doing an in-memory .find() on the client for a single tree_id.
CREATE OR REPLACE FUNCTION public.adoption_count_for_tree(t_id text)
RETURNS integer
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT COUNT(*)::integer FROM trees_adopted WHERE tree_id = t_id;
$$;
