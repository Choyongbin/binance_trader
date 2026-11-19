package trading

import "time"

// chronologicalIDLess provides a total order for immutable diagnostics.
// Timestamps can legitimately collide (coarse legacy clocks, imported
// archives, deterministic tests), so randomized archive IDs are the final
// deterministic tie-breaker instead of Go map iteration order.
func chronologicalIDLess(a time.Time, aID string, b time.Time, bID string) bool {
	if a.Equal(b) {
		return aID < bID
	}
	return a.Before(b)
}
