package trading

// Read-only, fail-closed restart/stream-gap incident inventory. This is an
// operator's evidence checklist, not an orphan-lock cleaner or permission to
// resume a monitor. In particular, timestamp-associated legacy markers cannot
// establish that an event or a fill was not missed.
import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// RestartForensics deliberately excludes session IDs, filenames, raw account
// events, HTTP errors, credentials, and any local issues provided by a caller.
// Snapshot digests are local change detectors, NOT exchange attestations.
type RestartForensics struct {
	Schema                       int                    `json:"schema"`
	CheckedAt                    time.Time              `json:"checked_at"`
	MonitorState                 string                 `json:"monitor_state"`
	MonitorLastReason            string                 `json:"monitor_last_reason"`
	MonitorEpoch                 int                    `json:"monitor_epoch"`
	MonitorSessionCount          int                    `json:"monitor_session_count"`
	MonitorCheckpointCount       int                    `json:"monitor_checkpoint_count"`
	MonitorEpochWindows          []MonitorEpochForensic `json:"monitor_epoch_windows"`
	MonitorArchiveSHA256         string                 `json:"monitor_archive_sha256"`
	MonitorLockPresent           bool                   `json:"monitor_lock_present"`
	EventLockPresent             bool                   `json:"event_lock_present"`
	EventCount                   int                    `json:"event_count"`
	EventArchiveSHA256           string                 `json:"event_archive_sha256"`
	GapMarkerCount               int                    `json:"gap_marker_count"`
	GapMarkerSHA256              string                 `json:"gap_marker_sha256"`
	HistoricalDiagnosticSessions int                    `json:"historical_diagnostic_sessions"`
	TimestampUnmatchedGaps       int                    `json:"timestamp_unmatched_gaps"`
	IncompleteHistoricalSessions int                    `json:"incomplete_historical_sessions"`
	InterruptedOrActive          bool                   `json:"interrupted_or_active"`
	RestartDisposition           string                 `json:"restart_disposition"`
	EvidenceStable               bool                   `json:"evidence_stable"`
	AutoRearmAllowed             bool                   `json:"auto_rearm_allowed"`
	TradingAuthorized            bool                   `json:"trading_authorized"`
	StreamContinuityProven       bool                   `json:"stream_continuity_proven"`
	ManualReviewRequired         bool                   `json:"manual_review_required"`
	Note                         string                 `json:"note"`
}

// snapshotDir inventories every byte including owner.json without disclosing
// names or contents. Unknown files/symlinks are refused. A second inventory
// after all parsing detects a concurrently changing local archive. Hashes are
// deliberately length-delimited and deterministic on Windows and Linux.
func snapshotDir(root string, validate func(dir, name string, isDir bool) bool) (string, int, int, error) {
	if info, e := os.Lstat(root); e == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", 0, 0, errors.New("invalid forensic root directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return "", 0, 0, e
	}
	h := sha256.New()
	total, sessions := 0, 0
	var walk func(string, string) error
	walk = func(path, rel string) error {
		entries, err := os.ReadDir(path)
		if errors.Is(err, os.ErrNotExist) && rel == "" {
			return nil
		}
		if err != nil {
			return err
		}
		if len(entries) > 100000 {
			return errors.New("forensic directory entry safety limit")
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, ent := range entries {
			if ent.Type()&os.ModeSymlink != 0 {
				return errors.New("forensic archive symlink")
			}
			next := ent.Name()
			if rel != "" {
				next = rel + "/" + next
			}
			isDir := ent.IsDir()
			if !validate(rel, ent.Name(), isDir) {
				return errors.New("unknown forensic archive entry")
			}
			kind := "F"
			if isDir {
				kind = "D"
			}
			_, _ = fmt.Fprintf(h, "%d:%s:%s;", len(next), next, kind)
			if isDir {
				if rel == "" && ent.Name() != "exclusive.active" {
					sessions++
				}
				if err := walk(filepath.Join(path, ent.Name()), next); err != nil {
					return err
				}
				continue
			}
			if !ent.Type().IsRegular() {
				return errors.New("non-regular forensic archive entry")
			}
			// No raw payload leaves this function; validate bounded records.
			info, err := ent.Info()
			if err != nil || info.Size() > 65536 {
				return errors.New("oversized forensic archive entry")
			}
			raw, err := os.ReadFile(filepath.Join(path, ent.Name()))
			if err != nil || len(raw) > 65536 {
				return errors.New("forensic archive read failure")
			}
			digest := sha256.Sum256(raw)
			_, _ = fmt.Fprintf(h, "%d:%x;", len(raw), digest)
			total++
			if total > 200000 {
				return errors.New("forensic file safety limit")
			}
		}
		return nil
	}
	if err := walk(root, ""); err != nil {
		return "", 0, 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), total, sessions, nil
}

func monitorTreeFingerprint(dataDir string) (string, int, int, error) {
	return snapshotDir(monitorRoot(dataDir), func(parent, name string, isDir bool) bool {
		switch {
		case parent == "" && name == "exclusive.active":
			return isDir
		case parent == "" && monitorSessionName.MatchString(name):
			return isDir
		case parent == "exclusive.active":
			return !isDir && name == "owner.json"
		case monitorSessionName.MatchString(parent):
			return !isDir && monitorCheckpointName.MatchString(name)
		default:
			return false
		}
	})
}

func eventTreeFingerprint(dataDir string) (string, int, error) {
	digest, n, _, err := snapshotDir(filepath.Join(dataDir, "trading", "user_events"), func(parent, name string, isDir bool) bool {
		switch {
		case parent == "" && name == "exclusive.active":
			return isDir
		case parent == "" && eventName.MatchString(name):
			return !isDir
		default:
			return false // event store lock is empty by contract
		}
	})
	return digest, n, err
}

func lockPresent(path string) (bool, error) {
	stat, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !stat.IsDir() || stat.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("invalid forensic lock")
	}
	return true, nil
}

// AnalyzeRestartForensics reads ALL hashed monitor checkpoints, validates every
// stored normalized event and legacy gap, then rehashes both trees to reject
// a torn/concurrently modified snapshot. No network calls; NO lock removal.
// Server GET while a monitor is active may fail due to concurrent writes: the
// operator should run the standalone CLI on an offline COPY of data later.
func AnalyzeRestartForensics(dataDir string, now time.Time) (RestartForensics, error) {
	out := RestartForensics{Schema: 1, CheckedAt: now.UTC(), ManualReviewRequired: true, MonitorEpochWindows: []MonitorEpochForensic{},
		Note: "과거 기록과 현재 연결은 별개입니다. 락/마커를 제거하거나 거래를 활성화하지 마십시오."}
	if dataDir == "" || now.IsZero() {
		return out, errors.New("forensic directory and time required")
	}
	beforeMonitor, _, _, err := monitorTreeFingerprint(dataDir)
	if err != nil {
		return out, err
	}
	beforeEvents, _, err := eventTreeFingerprint(dataDir)
	if err != nil {
		return out, err
	}
	cp, stale, err := ReadContinuousMonitor(dataDir) // ALL session chains checked
	if err != nil {
		return out, err
	}
	gap, err := AnalyzeStreamGaps(dataDir, now) // ALL legacy diagnostic archives verified
	if err != nil {
		return out, err
	}
	markers, _, err := scanGapMarkers(dataDir)
	if err != nil {
		return out, err
	}
	windows, err := monitorEpochWindows(dataDir, markers)
	if err != nil {
		return out, err
	}
	// A directory may have never been created on a fresh new installation.
	eventDir := filepath.Join(dataDir, "trading", "user_events")
	events, err := os.Lstat(eventDir)
	if err == nil {
		if !events.IsDir() || events.Mode()&os.ModeSymlink != 0 {
			return out, errors.New("invalid event store directory")
		}
		recorded, e := (&EventStore{dir: eventDir}).ReadAll()
		if e != nil {
			return out, e
		}
		out.EventCount = len(recorded)
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	out.MonitorLockPresent, err = lockPresent(filepath.Join(monitorRoot(dataDir), "exclusive.active"))
	if err != nil {
		return out, err
	}
	out.EventLockPresent, err = lockPresent(filepath.Join(eventDir, "exclusive.active"))
	if err != nil {
		return out, err
	}
	afterMonitor, files, sessions, err := monitorTreeFingerprint(dataDir)
	if err != nil {
		return out, err
	}
	afterEvents, _, err := eventTreeFingerprint(dataDir)
	if err != nil {
		return out, err
	}
	// Also refuse a gap change during this wider multi-directory audit.
	_, afterGapDigest, err := scanGapMarkers(dataDir)
	if err != nil {
		return out, err
	}
	if beforeMonitor != afterMonitor || beforeEvents != afterEvents || afterGapDigest != gap.MarkerDigest {
		return out, errors.New("forensic evidence changed during inspection")
	}
	// Recheck the lock-state and monitor archive validation to fail closed on
	// interrupted/partially written transitions across the inspection.
	cp2, stale2, err := ReadContinuousMonitor(dataDir)
	if err != nil || stale != stale2 || cp.SessionID != cp2.SessionID || cp.Sequence != cp2.Sequence || cp.SHA256 != cp2.SHA256 {
		return out, errors.New("monitor changed during forensic inspection")
	}
	out.EvidenceStable = true
	out.MonitorState = cp.State
	out.MonitorLastReason = cp.Reason // only trusted fixed whitelist or NOT_RUN
	out.MonitorEpoch = cp.Epoch
	out.MonitorEpochWindows = windows
	out.MonitorSessionCount = sessions
	if out.MonitorLockPresent {
		files--
	} // owner.json is not a checkpoint
	out.MonitorCheckpointCount = files
	out.MonitorArchiveSHA256 = afterMonitor
	out.EventArchiveSHA256 = afterEvents
	out.GapMarkerCount = gap.MarkerCount
	out.GapMarkerSHA256 = gap.MarkerDigest
	out.HistoricalDiagnosticSessions = gap.SessionCount
	out.TimestampUnmatchedGaps = gap.OutsideHistoricalSessions
	out.IncompleteHistoricalSessions = gap.IncompleteSessionCount
	out.InterruptedOrActive = stale || out.MonitorLockPresent || out.EventLockPresent || gap.IncompleteSessionCount != 0
	switch {
	case out.InterruptedOrActive:
		out.RestartDisposition = "OFFLINE_MANUAL_REVIEW_REQUIRED"
	case sessions == 0:
		out.RestartDisposition = "NO_MONITOR_HISTORY"
	default:
		out.RestartDisposition = "HISTORICAL_ONLY_REARM_REQUIRES_PASSPHRASE"
	}
	// Never export a readiness field that can be mistaken for exchange approval.
	return out, nil
}
