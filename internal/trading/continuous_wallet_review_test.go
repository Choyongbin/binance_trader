package trading

import (
	"strings"
	"testing"
	"time"
)

func TestContinuousWalletReviewNeverCallsAllocationIsolationVerified(t *testing.T) {
	dataDir := t.TempDir()
	baseline := recoveryBaseline(time.Now().UTC())
	issues, e := ReviewContinuousWalletExposure(dataDir, strings.Repeat("a", 64), baseline)
	if e != nil || !issueHas(issues, "분리") {
		t.Fatalf("internal budget incorrectly treated as isolated %v %v", issues, e)
	}
	baseline.Portfolio.AvailableBalance = "0"
	issues, e = ReviewContinuousWalletExposure(dataDir, strings.Repeat("a", 64), baseline)
	if e != nil || !issueHas(issues, "분리") {
		t.Fatalf("no-capital wallet gave trust %v %v", issues, e)
	}
	if _, e = ReviewContinuousWalletExposure(dataDir, "bad", baseline); e == nil {
		t.Fatal("account identity not checked")
	}
}
