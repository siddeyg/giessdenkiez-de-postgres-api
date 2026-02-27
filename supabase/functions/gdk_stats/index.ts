import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { loadEnvVars } from "../_shared/check-env.ts";
import { GdkError, ErrorTypes } from "../_shared/errors.ts";

const ENV_VARS = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "PUMPS_URL"];
const [SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, PUMPS_URL] =
	loadEnvVars(ENV_VARS);

const supabaseServiceRoleClient = createClient(
	SUPABASE_URL,
	SUPABASE_SERVICE_ROLE_KEY
);

// Cache is considered fresh for 24 hours. The first request after expiry
// triggers a synchronous recompute (refresh_gdk_stats_cache RPC), which then
// serves all subsequent requests for the next 24 hours instantly.
const CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;

const getPumpsCount = async (): Promise<number> => {
	const response = await fetch(PUMPS_URL);
	if (response.status !== 200) {
		throw new GdkError(response.statusText, ErrorTypes.GdkStatsPump);
	}
	const geojson = await response.json();
	return geojson.features.length;
};

const getCachedPayload = async (): Promise<Record<string, unknown> | null> => {
	const { data, error } = await supabaseServiceRoleClient
		.from("gdk_stats_cache")
		.select("payload, computed_at")
		.maybeSingle();

	if (error || !data) return null;

	const ageMs = Date.now() - new Date(data.computed_at).getTime();
	if (ageMs > CACHE_MAX_AGE_MS) return null; // stale — trigger recompute

	return data.payload as Record<string, unknown>;
};

const refreshCache = async (): Promise<Record<string, unknown>> => {
	// Recompute all DB-derived stats in a single SQL function call and store
	// the result in gdk_stats_cache. Then read back the freshly written row.
	const { error: rpcError } = await supabaseServiceRoleClient
		.rpc("refresh_gdk_stats_cache");

	if (rpcError) {
		throw new GdkError(rpcError.message, ErrorTypes.GdkStatsWatering);
	}

	const { data, error } = await supabaseServiceRoleClient
		.from("gdk_stats_cache")
		.select("payload")
		.single();

	if (error || !data) {
		throw new GdkError(
			"Cache refresh succeeded but reading result failed",
			ErrorTypes.GdkStatsWatering
		);
	}

	return data.payload as Record<string, unknown>;
};

const handler = async (request: Request): Promise<Response> => {
	if (request.method === "OPTIONS") {
		return new Response(null, { headers: corsHeaders, status: 204 });
	}

	try {
		// Fetch cache and pumps count in parallel.
		// pumps come from an external URL (Supabase Storage) and cannot be
		// cached in SQL, so they are always fetched live and merged in.
		const [cachedPayload, numPumps] = await Promise.all([
			getCachedPayload(),
			getPumpsCount(),
		]);

		const dbPayload = cachedPayload ?? await refreshCache();
		const stats = { ...dbPayload, numPumps };

		return new Response(JSON.stringify(stats), {
			status: 200,
			headers: {
				...corsHeaders,
				"Content-Type": "application/json",
			},
		});
	} catch (error) {
		if (error instanceof GdkError) {
			console.error(
				`Error of type ${error.errorType} in gdk_stats function invocation: ${error.message}`
			);
		} else {
			console.error(JSON.stringify(error));
		}

		return new Response(JSON.stringify(error), {
			status: 500,
			headers: {
				...corsHeaders,
				"Content-Type": "application/json",
			},
		});
	}
};

Deno.serve(handler);
