package trading

import (
	"os"
	"testing"
	"time"
)

func TestExternalClosePostingGateBothSidesNeverAuthorize(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, store, rehearsal := settlementProposalFixture(t, side)
			p, err := store.Prepare(rehearsal.EntryClientOrderID, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			before, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.LoadExternalClosePostingGate(p.ClientOrderID)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateExternalClosePostingGate(result); err != nil {
				t.Fatal(err)
			}
			if len(result.Blockers) != 7 || result.PostingAuthorized || result.TradingAuthorized {
				t.Fatalf("unsafe gate %+v", result)
			}
			repeat, err := store.LoadExternalClosePostingGate(p.ClientOrderID)
			if err != nil || repeat.DigestSHA256 != result.DigestSHA256 {
				t.Fatalf("non-deterministic %v", err)
			}
			after, err := snapshotExecutionEvidence(dir)
			if err != nil || before != after {
				t.Fatal("execution evidence mutated", err)
			}
			if _, err := os.Stat(store.path()); err != nil {
				t.Fatal(err)
			}
			result.TradingAuthorized = true
			if ValidateExternalClosePostingGate(result) == nil {
				t.Fatal("trading authorization forged")
			}
		})
	}
}

func TestExternalClosePostingGateFailsForUnsealedProposal(t *testing.T) {
	if _, err := PreviewExternalClosePosting(ExternalCloseSettlementProposal{}); err == nil {
		t.Fatal("invalid proposal accepted")
	}
	dir, store, r := settlementProposalFixture(t, Short)
	_ = dir
	if _, err := store.LoadExternalClosePostingGate(r.EntryClientOrderID); err == nil {
		t.Fatal("missing proposal accepted")
	}
}
