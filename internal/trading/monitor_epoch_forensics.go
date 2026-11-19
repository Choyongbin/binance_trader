package trading

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MonitorEpochForensic is a *candidate* association of a checkpoint window
// with legacy, anonymous marker timestamps. The epoch number is local to a
// randomized session; its identifier is intentionally not exported.
type MonitorEpochForensic struct {
	Start                     time.Time `json:"start"`
	End                       time.Time `json:"end,omitempty"`
	Epoch                     int       `json:"epoch"`
	SignedRESTSamples         int       `json:"signed_rest_samples"`
	LastState                 string    `json:"last_state"`
	EndReason                 string    `json:"end_reason"`
	TimestampCandidateMarkers int       `json:"timestamp_candidate_markers"`
	Incomplete                bool      `json:"incomplete"`
	Attribution               string    `json:"attribution"`
}

// Called ONLY between two full archive-tree fingerprint comparisons, and
// after ReadContinuousMonitor has validated every checkpoint in all sessions.
// An interrupted window is never attributed markers: timestamps alone cannot
// decide whether an unexpected socket drop swallowed earlier events.
func monitorEpochWindows(dataDir string, markers []gapMarker) ([]MonitorEpochForensic, error) {
	dir := monitorRoot(dataDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []MonitorEpochForensic{}, nil
	}
	if err != nil {
		return nil, err
	}
	windows := []MonitorEpochForensic{}
	for _, entry := range entries {
		if entry.Name() == "exclusive.active" {
			continue
		}
		// The complete-chain validator rejects invalid directories/entries.
		if !entry.IsDir() || !monitorSessionName.MatchString(entry.Name()) {
			return nil, errors.New("unexpected monitor epoch archive")
		}
		parts, err := os.ReadDir(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var current *MonitorEpochForensic
		for _, part := range parts {
			raw, err := os.ReadFile(filepath.Join(dir, entry.Name(), part.Name()))
			if err != nil {
				return nil, err
			}
			var cp MonitorCheckpoint
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.DisallowUnknownFields()
			if dec.Decode(&cp) != nil || dec.Decode(new(any)) != io.EOF || cp.SHA256 != cp.digest() {
				return nil, errors.New("monitor checkpoint changed during epoch inspection")
			}
			if cp.State == MonitorConnecting && cp.Epoch > 0 {
				// An unexpected new epoch while the old one was never closed
				// makes the previous window incomplete rather than harmless.
				if current != nil {
					current.Incomplete = true
					windows = append(windows, *current)
				}
				current = &MonitorEpochForensic{Start: cp.At, Epoch: cp.Epoch, LastState: cp.State,
					Attribution: "MONITOR_TIMESTAMP_CANDIDATE_NOT_CLEARED"}
			}
			if current == nil {
				continue
			}
			current.LastState = cp.State
			if cp.Reason == "REST_REFRESH" && cp.State == MonitorConnected {
				current.SignedRESTSamples++
			}
			if cp.State == MonitorUncertain && cp.Reason == "SOCKET_DROPPED" || cp.State == MonitorStopped || cp.State == MonitorBlocked {
				current.End = cp.At
				current.EndReason = cp.Reason
				windows = append(windows, *current)
				current = nil
			}
		}
		if current != nil {
			current.Incomplete = true
			windows = append(windows, *current)
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].Start.Before(windows[j].Start) })
	for _, marker := range markers {
		matches := []int{}
		for i, w := range windows {
			if w.Incomplete || w.End.IsZero() {
				continue
			}
			if !marker.at.Before(w.Start) && !marker.at.After(w.End) {
				matches = append(matches, i)
			}
		}
		// Strict unique attribution among ONLY monitor windows. It may also
		// overlap a historical recovery interval; still never claim clearance.
		if len(matches) == 1 {
			windows[matches[0]].TimestampCandidateMarkers++
		}
	}
	return windows, nil
}
