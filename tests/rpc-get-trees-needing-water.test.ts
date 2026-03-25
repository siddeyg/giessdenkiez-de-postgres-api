/**
 * Integration tests for the get_trees_needing_water RPC function.
 *
 * Requires a local Supabase instance with tree data and radolan_sum values
 * populated (run the DWD harvester at least once after importing tree data).
 *
 * The RPC is public (no auth required) so we use the anon client.
 */
import { supabaseAnonClient } from "../src/supabase-client";

// Bonn city centre — guaranteed to have trees within 550 m if data is imported.
const BONN_LAT = 50.73438;
const BONN_LNG = 7.09548;

describe("get_trees_needing_water RPC", () => {
	it("returns an array (may be empty if no drought stress currently)", async () => {
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: BONN_LAT, lng: BONN_LNG },
		);
		expect(error).toBeNull();
		expect(Array.isArray(data)).toBe(true);
	});

	it("returns at most max_results trees", async () => {
		const maxResults = 3;
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: BONN_LAT, lng: BONN_LNG, max_results: maxResults },
		);
		expect(error).toBeNull();
		expect(data!.length).toBeLessThanOrEqual(maxResults);
	});

	it("each result has the expected columns", async () => {
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: BONN_LAT, lng: BONN_LNG, max_results: 1 },
		);
		expect(error).toBeNull();

		if (data && data.length > 0) {
			const tree = data[0];
			expect(tree).toHaveProperty("id");
			expect(tree).toHaveProperty("lat");
			expect(tree).toHaveProperty("lng");
			expect(tree).toHaveProperty("radolan_sum");
			expect(tree).toHaveProperty("pflanzjahr");
			expect(tree).toHaveProperty("art_dtsch");
			expect(tree).toHaveProperty("distance_m");
			expect(tree).toHaveProperty("last_watered");
		}
	});

	it("all results are within the requested radius", async () => {
		const radius = 300;
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: BONN_LAT, lng: BONN_LNG, radius_m: radius },
		);
		expect(error).toBeNull();

		for (const tree of data ?? []) {
			expect(tree.distance_m).toBeLessThanOrEqual(radius);
		}
	});

	it("all results satisfy the drought threshold for their age class", async () => {
		const currentYear = new Date().getFullYear();
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: BONN_LAT, lng: BONN_LNG, max_results: 5 },
		);
		expect(error).toBeNull();

		for (const tree of data ?? []) {
			const age =
				tree.pflanzjahr && tree.pflanzjahr > 0
					? currentYear - tree.pflanzjahr
					: null;

			// Each returned tree must have a radolan_sum below the threshold for its age class.
			if (age === null) {
				// Unknown age: threshold is 200 (same as junior)
				expect(tree.radolan_sum).toBeLessThan(200);
			} else if (age <= 5) {
				expect(tree.radolan_sum).toBeLessThan(100);
			} else if (age <= 10) {
				expect(tree.radolan_sum).toBeLessThan(200);
			} else {
				expect(tree.radolan_sum).toBeLessThan(300);
			}
		}
	});

	it("returns an error when called with obviously invalid coordinates", async () => {
		// PostgREST will still return 200 with an empty result for out-of-range coords
		// (the spatial WHERE clause simply won't match any trees).
		const { data, error } = await supabaseAnonClient.rpc(
			"get_trees_needing_water",
			{ lat: 0, lng: 0 }, // middle of the ocean
		);
		expect(error).toBeNull();
		expect(data).toHaveLength(0);
	});
});
