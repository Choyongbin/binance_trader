package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/protectionfence"
)

func testProductSignedFenceReader(t *testing.T, response string, responseCode int) (productFenceSignedAlgoReader, protectionfence.Intent, *atomic.Int32) {
	t.Helper()
	var mutations atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
			return
		}
		if r.Method != http.MethodGet {
			mutations.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/fapi/v1/algoOrder" || r.URL.Query().Get("clientAlgoId") != "tp_v13_01" {
			t.Errorf("unexpected signed Algo lookup: %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(responseCode)
		fmt.Fprint(w, response)
	}))
	t.Cleanup(srv.Close)
	key := "demo-v13-test"
	h := sha256.Sum256([]byte(key))
	fingerprint := hex.EncodeToString(h[:])
	intent := protectionfence.Intent{
		Environment: "demo", GroupID: "entry_v13_1", Operation: protectionfence.SubmitTP,
		Symbol: "BTCUSDT", PositionSide: "LONG", QuantityBTC: "0.01",
		TargetClientAlgoID: "tp_v13_01", PlanSHA256: strings.Repeat("a", 64),
		AccountFingerprintSHA256: fingerprint, EvidenceSHA256: strings.Repeat("b", 64),
	}
	reader := productFenceSignedAlgoReader{
		client:             exchange.NewWithBase(srv.URL, srv.Client()),
		creds:              DemoCredentials{APIKey: key, Secret: "local-secret"},
		accountFingerprint: fingerprint,
		expected: ProtectionInstruction{ClientAlgoID: "tp_v13_01", Symbol: "BTCUSDT", Side: "SELL", PositionSide: Long,
			AlgoType: "CONDITIONAL", OrderType: "TAKE_PROFIT_MARKET", Quantity: "0.01", TriggerPrice: "105000", WorkingType: "CONTRACT_PRICE"},
	}
	return reader, intent, &mutations
}

func validProductSignedAlgoResponse() string {
	return `{"algoId":333,"clientAlgoId":"tp_v13_01","algoType":"CONDITIONAL",` +
		`"algoStatus":"NEW","symbol":"BTCUSDT","side":"SELL","positionSide":"LONG",` +
		`"orderType":"TAKE_PROFIT_MARKET","workingType":"CONTRACT_PRICE",` +
		`"quantity":"0.01","triggerPrice":"105000","closePosition":false,"reduceOnly":false}`
}

func TestProductSignedFenceRecoveryTwoReadsRemainNoReplay(t *testing.T) {
	reader, intent, mutations := testProductSignedFenceReader(t, validProductSignedAlgoResponse(), http.StatusOK)
	store, err := protectionfence.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SealBeforePossibleSend(intent, time.Now()); err != nil {
		t.Fatal(err)
	}
	review, err := store.InspectRecoveryOnce(context.Background(), reader)
	if err != nil || review.State != protectionfence.RecoverySubmitObserved || review.TradingAuthorized ||
		review.SubmitAuthorized || review.CancelAuthorized || review.RetryAuthorized ||
		review.SettlementAuthorized || !review.ManualReviewRequired || mutations.Load() != 0 {
		t.Fatalf("unsafe review %+v err=%v mutations=%d", review, err, mutations.Load())
	}
	if _, err := store.SealBeforePossibleSend(intent, time.Now()); err != protectionfence.ErrOutcomeUnknown {
		t.Fatalf("replay not blocked: %v", err)
	}
}

func TestProductSignedFenceRecoveryNotFoundRemainsUnproven(t *testing.T) {
	reader, intent, mutations := testProductSignedFenceReader(t, `{"code":-2013}`, http.StatusBadRequest)
	store, err := protectionfence.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SealBeforePossibleSend(intent, time.Now()); err != nil {
		t.Fatal(err)
	}
	review, err := store.InspectRecoveryOnce(context.Background(), reader)
	if err != nil || review.State != protectionfence.RecoveryAbsentUnproven || review.RetryAuthorized || mutations.Load() != 0 {
		t.Fatalf("unsafe absent review %+v err=%v mutations=%d", review, err, mutations.Load())
	}
}

func TestProductSignedFenceReaderRejectsWrongExchangeProtection(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"wrong type", strings.Replace(validProductSignedAlgoResponse(), "TAKE_PROFIT_MARKET", "STOP_MARKET", 1)},
		{"wrong side", strings.Replace(validProductSignedAlgoResponse(), `"SELL"`, `"BUY"`, 1)},
		{"wrong quantity", strings.Replace(validProductSignedAlgoResponse(), `"0.01"`, `"0.02"`, 1)},
		{"wrong trigger", strings.Replace(validProductSignedAlgoResponse(), `"105000"`, `"104000"`, 1)},
		{"wrong client algo", strings.Replace(validProductSignedAlgoResponse(), "tp_v13_01", "foreign", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, intent, mutations := testProductSignedFenceReader(t, tc.body, http.StatusOK)
			if _, err := reader.ReadSignedAlgo(context.Background(), intent); err == nil || mutations.Load() != 0 {
				t.Fatalf("bad signed instruction accepted: %v", err)
			}
		})
	}
}

func TestProductSignedFenceReaderRejectsWrongFingerprintBeforeGET(t *testing.T) {
	reader, intent, mutations := testProductSignedFenceReader(t, validProductSignedAlgoResponse(), http.StatusOK)
	intent.AccountFingerprintSHA256 = strings.Repeat("c", 64)
	if _, err := reader.ReadSignedAlgo(context.Background(), intent); err == nil || mutations.Load() != 0 {
		t.Fatalf("wrong fingerprint accepted: %v", err)
	}
}

func TestProductSignedFenceReaderRejectsWrongCancelAlgoID(t *testing.T) {
	reader, intent, mutations := testProductSignedFenceReader(t, validProductSignedAlgoResponse(), http.StatusOK)
	intent.Operation = protectionfence.CancelSibling
	intent.TargetAlgoID = 999 // observed is 333
	if _, err := reader.ReadSignedAlgo(context.Background(), intent); err == nil || mutations.Load() != 0 {
		t.Fatalf("cancel target Algo ID mismatch accepted: %v", err)
	}
}
