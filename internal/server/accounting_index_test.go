package server

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/reportindex"
	"binance_trader/internal/sqlitebridge"
	"binance_trader/internal/trading"
)

func TestReadOnlySQLiteAccountingEndpointsFailClosed(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("SQLite 없음: 실 DB 검증 생략")
	}
	data := t.TempDir()
	srv, e := newWithFetcher(data, readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	get := func(path string) (int, string) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8080"+path, nil)
		srv.Handler().ServeHTTP(w, req)
		return w.Code, w.Body.String()
	}
	if status, b := get("/api/v1/trading/accounting/history"); status != 200 || !strings.Contains(b, `"reports":[]`) {
		t.Fatalf("no records status=%d body=%s", status, b)
	}
	now := time.Now().UTC()
	r := trading.AccountingReport{Schema: 1, CreatedAt: now, WindowStartMS: now.Add(-time.Minute).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(), Owners: []trading.OwnerAccounting{
		{Source: trading.Model, ModelID: "EMA-test", Symbol: "BTCUSDT", PositionSide: trading.Long, CommissionByAsset: map[string]string{"USDT": "0.00000001"}, VirtualGrossRealizedUSDT: "12.12345678"},
	}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{}, FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS", Warnings: []string{"실시간 손익 아님", "미배분 펀딩 누락"}}
	if e = trading.ArchiveAccounting(data, &r); e != nil {
		t.Fatal(e)
	}
	if status, _ := get("/api/v1/trading/accounting/history"); status != 409 {
		t.Fatalf("must not auto build index: %d", status)
	}
	if _, e = reportindex.Build(data); e != nil {
		t.Fatal(e)
	}
	if status, b := get("/api/v1/trading/accounting/index/status"); status != 200 || !strings.Contains(b, `"state":"CURRENT"`) {
		t.Fatalf("index status %d %s", status, b)
	}
	status, b := get("/api/v1/trading/accounting/history?source=model&model_id=EMA-test&limit=10&offset=0")
	if status != 200 {
		t.Fatalf("history failed %d %s", status, b)
	}
	var decoded map[string]json.RawMessage
	if e = json.Unmarshal([]byte(b), &decoded); e != nil || !strings.Contains(b, `"total":1`) || !strings.Contains(b, `"virtual_gross_realized_usdt":"12.12345678"`) || !strings.Contains(b, `"trading_authorized":false`) {
		t.Fatalf("history data %s / %v", b, e)
	}
	for _, path := range []string{"/api/v1/trading/accounting/history?source=model%27%3BDROP%20TABLE%20reports%3B--", "/api/v1/trading/accounting/history?limit=2&limit=5", "/api/v1/trading/accounting/history?model_id=A", "/api/v1/trading/accounting/history?unexpected=1"} {
		if status, _ = get(path); status != 400 {
			t.Errorf("accepted bad filter %s: %d", path, status)
		}
	}
	if status, _ = get("/api/v1/trading/accounting/index/status?inject=1"); status != 400 {
		t.Fatalf("bad index query %d", status)
	}
	p := filepath.Join(data, "trading", "report_index")
	items, e := os.ReadDir(p)
	if e != nil {
		t.Fatal(e)
	}
	for _, item := range items {
		if strings.HasSuffix(item.Name(), ".sqlite3") {
			f, e := os.OpenFile(filepath.Join(p, item.Name()), os.O_APPEND|os.O_WRONLY, 0600)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = f.Write([]byte("broken")); e != nil {
				t.Fatal(e)
			}
			f.Close()
			break
		}
	}
	if status, b := get("/api/v1/trading/accounting/history"); status != 503 || !strings.Contains(b, "손상") {
		t.Fatalf("corrupt index should block %d %s", status, b)
	}
	if status, _ := get("/api/v1/trading/accounting/index/status"); status != 503 {
		t.Fatalf("corrupt status %d", status)
	}
	if w := request(srv.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("manual unexpectedly enabled %d", w.Code)
	}
}
