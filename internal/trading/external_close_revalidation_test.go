package trading

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockCloseReader struct {
	v   SignedCloseEvidence
	err error
}

func (m mockCloseReader) RefreshExternalCloseEvidence(context.Context, int64, int64) (SignedCloseEvidence, error) {
	return m.v, m.err
}
func TestCloseRevalidationBothDirectionsReplayAndFailures(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			_, store, reh := settlementProposalFixture(t, side)
			p, err := store.Prepare(reh.EntryClientOrderID, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			ev := SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs, AccountFlat: true, ObservedAt: now}
			a, err := RevalidateExternalClose(context.Background(), p, mockCloseReader{v: ev}, now)
			if err != nil {
				t.Fatal(err)
			}
			b, err := RevalidateExternalClose(context.Background(), p, mockCloseReader{v: ev}, now)
			if err != nil || a.ReplayKey != b.ReplayKey || a.DigestSHA256 != b.DigestSHA256 {
				t.Fatal("unstable replay", err)
			}
			if a.PostingAuthorized || a.TradingAuthorized || !a.EvidenceMatched {
				t.Fatal("unauthorized", a)
			}
			a.PostingAuthorized = true
			if ValidateExternalCloseRevalidation(a) == nil {
				t.Fatal("forged authorization")
			}
			ev.ExitTradeIDs = []int64{999999}
			if _, err := RevalidateExternalClose(context.Background(), p, mockCloseReader{v: ev}, now); err == nil {
				t.Fatal("accepted changed fill")
			}
			ev.ExitTradeIDs = p.ExitTradeIDs
			ev.ObservedAt = now.Add(-3 * time.Minute)
			if _, err := RevalidateExternalClose(context.Background(), p, mockCloseReader{v: ev}, now); err == nil {
				t.Fatal("accepted stale evidence")
			}
			if _, err := RevalidateExternalClose(context.Background(), p, mockCloseReader{err: errors.New("timeout")}, now); err == nil {
				t.Fatal("accepted signed fetch error")
			}
		})
	}
}
