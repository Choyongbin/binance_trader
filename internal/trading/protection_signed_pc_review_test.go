package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binance_trader/internal/wallet"
)

func setupSignedPCBinding(t *testing.T, root string) (*wallet.Store, string) {
	t.Helper()
	s, err := wallet.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("test-demo"))
	fingerprint := hex.EncodeToString(hash[:])
	if _, err := s.Change("wallet-binding-fixture", "ALLOCATE", "100", "500", fingerprint); err != nil {
		t.Fatal(err)
	}
	return s, fingerprint
}
func TestProtectionSignedPCBothSidesBoundedNeverApproved(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			root, r, reader, since := newProtectionGateFixture(t, side)
			setupSignedPCBinding(t, root)
			review, err := ReviewPreparedProtectionSignedPC(context.Background(), root, r.ClientOrderID, reader, DemoCredentials{APIKey: "test-demo", Secret: "dummy"}, since)
			if err != nil {
				t.Fatal(err)
			}
			if review.ErrorCode != "" || !review.SignedGETAttempted || review.WalletBindingRevision != 1 || review.WalletBindingTailSHA256 == "" || reader.reads != 2 || review.Gate.State != ProtectionGateBoundedReview || review.TradingAuthorized || !review.ManualReviewRequired {
				t.Fatalf("unsafe review: %+v", review)
			}
			assertNeverApproved(t, review.Gate)
			raw, err := json.Marshal(review)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "dummy") || strings.Contains(string(raw), "test-demo") {
				t.Fatal("credential leaked into report")
			}
		})
	}
}
func TestProtectionSignedPCFailClosedBeforeNetwork(t *testing.T) {
	root, r, m, since := newProtectionGateFixture(t, Long)
	creds := DemoCredentials{APIKey: "test-demo", Secret: "dummy"}
	got, err := ReviewPreparedProtectionSignedPC(context.Background(), root, r.ClientOrderID, m, creds, since)
	if err == nil || got.ErrorCode != "ACCOUNT_NOT_BOUND" || got.SignedGETAttempted || m.reads != 0 {
		t.Fatalf("empty wallet must block: %+v %v", got, err)
	}
	_, bound := setupSignedPCBinding(t, root)
	if bound == "" {
		t.Fatal("binding empty")
	}
	got, err = ReviewPreparedProtectionSignedPC(context.Background(), root, r.ClientOrderID, m, DemoCredentials{APIKey: "different-demo", Secret: "dummy"}, since)
	if err == nil || got.ErrorCode != "ACCOUNT_BINDING_MISMATCH" || got.SignedGETAttempted || m.reads != 0 {
		t.Fatalf("wrong key must block: %+v %v", got, err)
	}
	got, err = ReviewPreparedProtectionSignedPC(context.Background(), root, "bt_bad", m, creds, since)
	if err == nil || got.ErrorCode != "INVALID_INPUT" || m.reads != 0 {
		t.Fatalf("bad ID must block: %+v %v", got, err)
	}
	if err = os.Remove(filepath.Join(root, "trading", "reservations", r.ClientOrderID+".json")); err != nil {
		t.Fatal(err)
	}
	got, err = ReviewPreparedProtectionSignedPC(context.Background(), root, r.ClientOrderID, m, creds, since)
	if err == nil || got.ErrorCode != "LOCAL_RESERVATION_UNAVAILABLE" || m.reads != 0 {
		t.Fatalf("missing file must block: %+v %v", got, err)
	}
}
func TestProtectionSignedPCSignedFailureAndDrift(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, root string, m *approvalGateSignedReader, s *wallet.Store, fingerprint string)
	}{
		{"first_get_failure", func(_ *testing.T, _ string, m *approvalGateSignedReader, _ *wallet.Store, _ string) { m.failAt = 1 }},
		{"second_get_failure", func(_ *testing.T, _ string, m *approvalGateSignedReader, _ *wallet.Store, _ string) { m.failAt = 2 }},
		{"wallet_changes_during_get", func(t *testing.T, _ string, m *approvalGateSignedReader, s *wallet.Store, fp string) {
			m.onRead = func(n int) {
				if n == 2 {
					if _, err := s.Change("wallet-binding-changed", "ALLOCATE", "1", "500", fp); err != nil {
						t.Fatal(err)
					}
				}
			}
		}},
		{"reservation_removed_during_get", func(t *testing.T, root string, m *approvalGateSignedReader, _ *wallet.Store, _ string) {
			m.onRead = func(n int) {
				if n == 2 {
					if err := os.Remove(filepath.Join(root, "trading", "reservations", "bt_"+strings.Repeat("0", 28)+".json")); err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
				}
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root, r, m, since := newProtectionGateFixture(t, Long)
			walletStore, fp := setupSignedPCBinding(t, root)
			if tt.name == "reservation_removed_during_get" {
				m.onRead = func(n int) {
					if n == 2 {
						if err := os.Remove(filepath.Join(root, "trading", "reservations", r.ClientOrderID+".json")); err != nil {
							t.Fatal(err)
						}
					}
				}
			} else {
				tt.setup(t, root, m, walletStore, fp)
			}
			review, err := ReviewPreparedProtectionSignedPC(context.Background(), root, r.ClientOrderID, m, DemoCredentials{APIKey: "test-demo", Secret: "dummy"}, since)
			if err == nil || review.Gate.State != ProtectionGateBlocked || review.TradingAuthorized || !review.ManualReviewRequired || !review.SignedGETAttempted {
				t.Fatalf("unsafe failure: %+v %v", review, err)
			}
			if tt.name == "wallet_changes_during_get" && review.ErrorCode != "ACCOUNT_BINDING_CHANGED" {
				t.Fatalf("wallet drift missed: %+v", review)
			}
			if tt.name == "reservation_removed_during_get" && review.ErrorCode != "LOCAL_RESERVATION_CHANGED" {
				t.Fatalf("reservation drift missed: %+v", review)
			}
		})
	}
}
