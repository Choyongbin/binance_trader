package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func orderEvent(numeric bool) []byte {
	payload := `{"e":"ORDER_TRADE_UPDATE","E":1790525396365,"T":1790525396364,"o":{"s":"BTCUSDT","c":"bt_test","i":1234,"t":223,"S":"BUY","ps":"LONG","x":"TRADE","X":"PARTIALLY_FILLED","z":"0.002","l":"0.002","L":"100000.50","n":"0.2","N":"USDT","rp":"0"}}`
	if numeric {
		payload = strings.ReplaceAll(payload, `"0.002"`, `0.002`)
		payload = strings.ReplaceAll(payload, `"100000.50"`, `100000.50`)
		payload = strings.ReplaceAll(payload, `"0.2"`, `0.2`)
		payload = strings.ReplaceAll(payload, `"0"`, `0`)
	}
	return []byte(payload)
}
func TestAccountDecoderPrecisionAndSparseEvents(t *testing.T) {
	for _, raw := range [][]byte{orderEvent(false), orderEvent(true)} {
		v, err := ParseAccountEvent(raw)
		if err != nil {
			t.Fatal(err)
		}
		if v.Order.LastQty != "0.002" || v.Order.LastPrice == "" || v.Order.OrderID != 1234 || v.Order.TradeID != 223 {
			t.Fatalf("unexpected parsed %+v", v)
		}
	}
	sparse := `{"e":"ACCOUNT_UPDATE","E":1790525396365,"T":1790525396364,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"111"}]}}`
	ev, err := ParseAccountEvent([]byte(sparse))
	if err != nil || ev.Account.Reason != "FUNDING_FEE" || len(ev.Account.PositionSymbols) != 0 {
		t.Fatalf("sparse funding event %+v err=%v", ev, err)
	}
	// Deliberately different JSON key ordering must yield identical digest.
	a, _ := ParseAccountEvent([]byte(`{"e":"listenKeyExpired","E":1790525396365,"listenKey":"PRIVATESECRET"}`))
	b, _ := ParseAccountEvent([]byte(`{"listenKey":"PRIVATESECRET","E":1790525396365,"e":"listenKeyExpired"}`))
	if a.Digest != b.Digest {
		t.Fatalf("canonical digest drift %s %s", a.Digest, b.Digest)
	}
	if strings.Contains(string(mustJSON(a)), "PRIVATESECRET") {
		t.Fatal("listenKey leaked in normalized event")
	}
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
func TestAccountDecoderFailClosed(t *testing.T) {
	cases := []string{
		`{}`, `{"e":"NEW_ORDERS","E":123}`, `{"e":"ORDER_TRADE_UPDATE","E":123}`, `{"e":"ACCOUNT_UPDATE","E":123,"a":{}}`,
		`{"e":"ORDER_TRADE_UPDATE","E":1790525396365,"o":{"s":"BTCUSDT","c":"x","i":1,"t":1,"S":"BUY","ps":"LONG","x":"TRADE","X":"FILLED","z":"0.001","l":"0.001","L":"1e200","n":"0","N":"USDT","rp":"0"}}`,
	}
	for _, s := range cases {
		if _, err := ParseAccountEvent([]byte(s)); err == nil {
			t.Fatalf("accepted malformed %s", s)
		}
	}
}
func TestEventStoreDedupeRecoveryTamperAndLock(t *testing.T) {
	d := t.TempDir()
	s, err := OpenEventStore(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenEventStore(d); err == nil {
		t.Fatal("second store must fail")
	}
	ev, err := ParseAccountEvent(orderEvent(false))
	if err != nil {
		t.Fatal(err)
	}
	if fresh, err := s.Append(ev); err != nil || !fresh {
		t.Fatalf("append=%v %v", fresh, err)
	}
	if fresh, err := s.Append(ev); err != nil || fresh {
		t.Fatalf("duplicate=%v %v", fresh, err)
	}
	if halted, _ := s.Status(); !halted {
		t.Fatal("stream event must not authorize trading")
	}
	all, err := s.ReadAll()
	if err != nil || len(all) != 1 {
		t.Fatalf("read %d %v", len(all), err)
	}
	if err := s.RecordDiscontinuity("DISCONNECT"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenEventStore(d)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := s.ReadAll(); e != nil || len(a) != 1 {
		t.Fatalf("reload %v %v", a, e)
	}
	if err := os.WriteFile(filepath.Join(d, "trading", "user_events", ev.Digest+".json"), []byte(`{"schema":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadAll(); err == nil {
		t.Fatal("corruption ignored")
	}
	s.Close()
	if _, err := OpenEventStore(d); err == nil {
		t.Fatal("corrupt store reopened")
	}
}
func TestEventStoreRejectsForgedDigest(t *testing.T) {
	s, err := OpenEventStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Append(AccountEvent{Digest: "../../../../../../../../evil00000000000000000000000000000000000000000000", Kind: "TRADE_LITE", EventTime: time.Now().UnixMilli()}); err == nil {
		t.Fatal("unsafe event name accepted")
	}
}

func TestAccountDecoderRecognizesCurrentAlgoUpdateAsReconciliationSignal(t *testing.T) {
	raw := []byte(`{"e":"ALGO_UPDATE","E":1790525396365,"T":1790525396364,"o":{"ai":123,"X":"NEW","s":"BTCUSDT"}}`)
	ev, err := ParseAccountEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "ALGO_UPDATE" || ev.EventTime != 1790525396365 || ev.Order != nil || ev.Account != nil || ev.Digest == "" {
		t.Fatalf("algo update must be metadata-only reconciliation signal: %+v", ev)
	}
}

func TestPrivateProbeArchiveAllowsCurrentAlgoUpdateAndLegacyTriggerReject(t *testing.T) {
	now := time.Now().UTC()
	for _, kind := range []string{"ALGO_UPDATE", "CONDITIONAL_ORDER_TRIGGER_REJECT"} {
		r := PrivateTransportResult{
			Schema: 1, Status: "EVENTS_OBSERVED", StartedAt: now, FinishedAt: now.Add(time.Millisecond),
			HandshakeOK: true, EventCount: 1, EventTypes: []string{kind}, HistoricalOnly: true,
			Note: privateProbeDisclaimer, DialPhase: "CONNECTED", DialRoute: "PRIVATE_PATH_LISTENKEY",
		}
		if _, err := ArchivePrivateProbe(t.TempDir(), r); err != nil {
			t.Fatalf("event %s rejected: %v", kind, err)
		}
	}
}
