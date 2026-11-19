package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func incidentDirs(t *testing.T) (string, string) {
	t.Helper()
	parent := t.TempDir()
	data := filepath.Join(parent, "copy-of-stopped-data")
	output := filepath.Join(parent, "separate-review-ledger")
	for _, p := range []string{data, output} {
		if e := os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	return data, output
}

func TestIncidentReviewTwoPhaseEvidenceOnlyAndReplayDenied(t *testing.T) {
	data, review := incidentDirs(t)
	now := time.Now().UTC()
	p, e := PrepareIncidentReview(data, review, now)
	if e != nil {
		t.Fatal(e)
	}
	if p.ReviewCategory != "NO_MONITOR_HISTORY" || p.MonitorLockPresent || p.TradingAuthorized || p.AutoRearmAllowed || p.StreamContinuityProven || !p.ManualReviewRequired || p.SHA256 != p.digest() {
		t.Fatalf("unsafe plan %+v", p)
	}
	if _, e := os.Stat(filepath.Join(data, "trading")); !os.IsNotExist(e) {
		t.Fatal("planning mutated inspected source")
	}
	if _, e = AcknowledgeIncidentReview(data, review, p.ID, "wrong", true, true, now.Add(time.Minute)); e == nil {
		t.Fatal("wrong challenge accepted")
	}
	if _, e = AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, false, now.Add(time.Minute)); e == nil {
		t.Fatal("missing external backup confirmation accepted")
	}
	if _, e = AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, false, true, now.Add(time.Minute)); e == nil {
		t.Fatal("unconfirmed stopped process accepted")
	}
	receipt, e := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	if receipt.Scope != "OFFLINE_EVIDENCE_ACKNOWLEDGEMENT_ONLY" || receipt.TradingAuthorized || receipt.AutoRearmAllowed || receipt.StreamContinuityProven || !receipt.ManualReviewRequired || receipt.PlanSHA256 != p.SHA256 {
		t.Fatalf("unsafe receipt %+v", receipt)
	}
	if _, e = AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, now.Add(2*time.Minute)); e == nil {
		t.Fatal("replay accepted")
	}
	plans, receipts, e := ReadIncidentReviews(review)
	if e != nil || len(plans) != 1 || len(receipts) != 1 || plans[0].ID != p.ID || receipts[0].SHA256 != receipt.SHA256 {
		t.Fatalf("verified ledger incomplete %v %v %v", plans, receipts, e)
	}
	if _, e := os.Stat(filepath.Join(data, "trading")); !os.IsNotExist(e) {
		t.Fatal("receipt wrote into inspected data tree")
	}
}

func TestIncidentReviewCrashOwnerCannotBeDeclaredStaleOrRemoved(t *testing.T) {
	data, review := incidentDirs(t)
	j, e := openMonitorJournal(data)
	if e != nil {
		t.Fatal(e)
	}
	cp := blankMonitor()
	cp.State, cp.Reason, cp.Epoch = MonitorConnecting, "HANDSHAKE", 1
	if e = j.append(&cp); e != nil {
		t.Fatal(e)
	}
	store, e := OpenEventStore(data)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.RecordDiscontinuity("STARTUP"); e != nil {
		t.Fatal(e)
	}
	baseline, e := AnalyzeRestartForensics(data, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	p, e := PrepareIncidentReview(data, review, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	if p.ReviewCategory != "OWNER_OR_HISTORY_UNVERIFIED" || !p.MonitorLockPresent || !p.EventLockPresent || p.HistoricalGapMarkers != 1 || p.ForensicDisposition != "OFFLINE_MANUAL_REVIEW_REQUIRED" {
		t.Fatalf("crashed owner was considered cleared %+v", p)
	}
	r, e := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	if r.AutoRearmAllowed || r.TradingAuthorized {
		t.Fatal("manual receipt activated trading")
	}
	after, e := AnalyzeRestartForensics(data, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if baseline.MonitorArchiveSHA256 != after.MonitorArchiveSHA256 || baseline.EventArchiveSHA256 != after.EventArchiveSHA256 || baseline.GapMarkerSHA256 != after.GapMarkerSHA256 || !after.MonitorLockPresent || !after.EventLockPresent {
		t.Fatal("operator acknowledgement modified source evidence")
	}
	if _, e := openMonitorJournal(data); e == nil {
		t.Fatal("operator evidence receipt allowed unsafe stale rearm")
	}
	if _, e := os.Stat(j.lock); e != nil {
		t.Fatal("monitor lock removed")
	}
	if _, e := os.Stat(store.lock); e != nil {
		t.Fatal("event lock removed")
	}
}

func TestIncidentReviewRejectsChangedEvidenceExpiredOrEditedArchives(t *testing.T) {
	cases := []string{"gap", "monitor", "event", "mutation_gate", "expired"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			data, review := incidentDirs(t)
			now := time.Now().UTC()
			p, e := PrepareIncidentReview(data, review, now)
			if e != nil {
				t.Fatal(e)
			}
			switch name {
			case "gap":
				s, err := OpenEventStore(data)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.RecordDiscontinuity("ROTATE"); err != nil {
					t.Fatal(err)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			case "monitor":
				j, err := openMonitorJournal(data)
				if err != nil {
					t.Fatal(err)
				}
				cp := blankMonitor()
				cp.State, cp.Reason = MonitorBlocked, "REST_FAILED"
				if err = j.append(&cp); err != nil {
					t.Fatal(err)
				}
				if err = j.close(); err != nil {
					t.Fatal(err)
				}
			case "event":
				// A new exclusive lock is itself relevant event-store evidence.
				s, err := OpenEventStore(data)
				if err != nil {
					t.Fatal(err)
				}
				_ = s
			case "mutation_gate":
				gate, err := AcquireMutationGate(data)
				if err != nil {
					t.Fatal(err)
				}
				_ = gate
			case "expired":
				// No mutation, just an expired challenge.
			}
			at := now.Add(time.Minute)
			if name == "expired" {
				at = now.Add(incidentPlanTTL + time.Second)
			}
			if _, e := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, at); e == nil {
				t.Fatal("changed or expired source was acknowledged")
			}
			_, receipts, e := ReadIncidentReviews(review)
			if e != nil || len(receipts) != 0 {
				t.Fatalf("partial acknowledgement despite error %v %v", receipts, e)
			}
		})
	}
}

func TestIncidentReviewRejectsTamperingSymlinksAndNestedDirectory(t *testing.T) {
	t.Run("edited plan", func(t *testing.T) {
		data, review := incidentDirs(t)
		p, e := PrepareIncidentReview(data, review, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(review, "plan-"+p.ID+".json")
		raw, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		edited := strings.Replace(string(raw), `"auto_rearm_allowed": false`, `"auto_rearm_allowed": true`, 1)
		if edited == string(raw) {
			t.Fatal("fixture did not tamper")
		}
		if e = os.WriteFile(path, []byte(edited), 0600); e != nil {
			t.Fatal(e)
		}
		if _, _, e = ReadIncidentReviews(review); e == nil {
			t.Fatal("tampered plan accepted")
		}
		if _, e = AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, time.Now()); e == nil {
			t.Fatal("tampered review accepted")
		}
	})
	t.Run("edited receipt", func(t *testing.T) {
		data, review := incidentDirs(t)
		p, e := PrepareIncidentReview(data, review, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		r, e := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(review, "receipt-"+p.ID+".json")
		raw, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		var parsed IncidentReviewReceipt
		if e = json.Unmarshal(raw, &parsed); e != nil {
			t.Fatal(e)
		}
		parsed.TradingAuthorized = true
		edited, _ := json.Marshal(parsed)
		if e = os.WriteFile(path, edited, 0600); e != nil {
			t.Fatal(e)
		}
		if _, _, e = ReadIncidentReviews(review); e == nil || r.TradingAuthorized {
			t.Fatal("tampered receipt passed")
		}
	})
	t.Run("nested output", func(t *testing.T) {
		data, _ := incidentDirs(t)
		inside := filepath.Join(data, "archive")
		if e := os.Mkdir(inside, 0700); e != nil {
			t.Fatal(e)
		}
		if _, e := PrepareIncidentReview(data, inside, time.Now()); e == nil {
			t.Fatal("review wrote inside user data")
		}
	})
	t.Run("source inside output", func(t *testing.T) {
		data, _ := incidentDirs(t)
		out := filepath.Dir(data)
		if _, e := PrepareIncidentReview(data, out, time.Now()); e == nil {
			t.Fatal("review parent overlapped source")
		}
	})
	t.Run("symlink output", func(t *testing.T) {
		data, _ := incidentDirs(t)
		alias := filepath.Join(filepath.Dir(data), "data-alias")
		if e := os.Symlink(data, alias); e != nil {
			t.Skipf("symlink not permitted: %v", e)
		}
		if _, e := PrepareIncidentReview(data, alias, time.Now()); e == nil {
			t.Fatal("symlink alias bypassed data/output boundary")
		}
	})
	t.Run("stale review lock", func(t *testing.T) {
		data, review := incidentDirs(t)
		if e := os.Mkdir(filepath.Join(review, "exclusive.active"), 0700); e != nil {
			t.Fatal(e)
		}
		if _, e := PrepareIncidentReview(data, review, time.Now()); e == nil {
			t.Fatal("stale reviewer lock auto-cleared")
		}
		if _, _, e := ReadIncidentReviews(review); e == nil {
			t.Fatal("reader ignored reviewer lock")
		}
	})
}

func TestIncidentReviewConcurrentAcknowledgementsSingleWriter(t *testing.T) {
	data, review := incidentDirs(t)
	p, e := PrepareIncidentReview(data, review, time.Now().UTC())
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	result := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, time.Now().UTC())
			result <- err
		}()
	}
	wg.Wait()
	close(result)
	succeeded, failed := 0, 0
	for err := range result {
		if err == nil {
			succeeded++
		} else {
			failed++
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent operator receipts success=%d failed=%d", succeeded, failed)
	}
	_, receipts, e := ReadIncidentReviews(review)
	if e != nil || len(receipts) != 1 {
		t.Fatalf("lost or duplicated receipt %v %v", receipts, e)
	}
}

func TestIncidentReviewOtherDiagnosticOwnersRemainUnverified(t *testing.T) {
	t.Run("active recovery probe", func(t *testing.T) {
		data, review := incidentDirs(t)
		probe, e := OpenRecoveryProbeStore(data)
		if e != nil {
			t.Fatal(e)
		}
		p, e := PrepareIncidentReview(data, review, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if !p.RecoveryProbeLockPresent || p.ReviewCategory != "OWNER_OR_HISTORY_UNVERIFIED" {
			t.Fatalf("active probe misclassified: %+v", p)
		}
		if _, e := os.Stat(probe.lock); e != nil {
			t.Fatal("probe owner deleted", e)
		}
		// A copied diagnostic from a stopped server may have this orphan.
		// Acknowledgement MUST NOT clear it.
		if _, e := AcknowledgeIncidentReview(data, review, p.ID, p.Confirmation, true, true, time.Now()); e != nil {
			t.Fatal(e)
		}
		if _, e := os.Stat(probe.lock); e != nil {
			t.Fatal("probe owner cleared by receipt")
		}
	})
	t.Run("active reconnect watch", func(t *testing.T) {
		data, review := incidentDirs(t)
		watch, e := openReconnectArchive(data)
		if e != nil {
			t.Fatal(e)
		}
		p, e := PrepareIncidentReview(data, review, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if !p.ReconnectWatchLockPresent || p.ReviewCategory != "OWNER_OR_HISTORY_UNVERIFIED" {
			t.Fatalf("active watch misclassified: %+v", p)
		}
		if _, e := os.Stat(watch.lock); e != nil {
			t.Fatal("watch owner deleted", e)
		}
	})
}
