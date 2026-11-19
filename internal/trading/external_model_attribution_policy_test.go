package trading

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExternalModelAttributionPolicyAmountsFundingAndRestart(t *testing.T) {
	a := crossFinancialFixture(t, Long, "model-policy-a", 3311, 5*time.Hour, 83311)
	b := crossFinancialFixture(t, Short, "model-policy-b", 3322, 3*time.Hour, 83322)
	ctx := context.Background()
	beforeA, err := snapshotExecutionEvidence(a.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeB, err := snapshotExecutionEvidence(b.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	input := []ExternalFinancialEventSource{b, a}
	got, err := InspectExternalModelAttributionPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateExternalModelAttributionPolicy(got); err != nil {
		t.Fatal(err)
	}
	if got.Stage != ExternalModelAttributionPolicyNotPosted || got.CrossEventAuditSHA256 == "" ||
		len(got.Incidents) != 2 || len(got.ProvisionalModels) != 1 ||
		got.AccountWideFundingUSDT != nil || got.SharedAccountProven || got.CompleteHistoryProven ||
		got.ModelOwnershipProven || got.FundingAllocated || got.PostingApplied ||
		got.SettlementAuthorized || got.TradingAuthorized ||
		got.DigestSHA256 != got.digest() {
		t.Fatalf("attribution escaped non-posting contract: %+v", got)
	}
	m := got.ProvisionalModels[0]
	if m.ModelFundingUSDT != nil || m.ModelFinalNetUSDT != nil ||
		m.EntryCommissionUSDT != "0.06" || m.ExitCommissionUSDT != "0.05" ||
		m.ExchangeRealizedUSDT != "2.5" || m.ProvisionalExFundingUSDT != "2.39" ||
		len(m.IncidentIDs) != 2 || m.IncidentIDs[0] >= m.IncidentIDs[1] {
		t.Fatalf("unproven funding or wrong bounded fees: %+v", m)
	}
	for _, inc := range got.Incidents {
		if inc.UnallocatedFundingUSDT != "-0.01" || len(inc.FundingTransactionIDs) != 1 {
			t.Fatalf("account funding may not be silently lost or model allocated: %+v", inc)
		}
	}
	encoded, err := json.Marshal(got)
	if err != nil || !strings.Contains(string(encoded), `"model_funding_usdt":null`) ||
		!strings.Contains(string(encoded), `"account_wide_funding_usdt":null`) ||
		!strings.Contains(string(encoded), `"settlement_authorized":false`) {
		t.Fatalf("incorrect or over-authorizing JSON: %v %s", err, encoded)
	}
	repeated, err := InspectExternalModelAttributionPolicy(ctx, []ExternalFinancialEventSource{a, b})
	if err != nil || !reflect.DeepEqual(got, repeated) {
		t.Fatalf("order/restart non-determinism: %v", err)
	}
	afterA, err := snapshotExecutionEvidence(a.DataDir)
	if err != nil || afterA != beforeA {
		t.Fatal("model policy wrote original source a")
	}
	afterB, err := snapshotExecutionEvidence(b.DataDir)
	if err != nil || afterB != beforeB {
		t.Fatal("model policy wrote original source b")
	}
}

func TestExternalModelAttributionPolicyRejectsConflictingAndForgedSources(t *testing.T) {
	a := crossFinancialFixture(t, Long, "policy-conflict-a", 3411, 5*time.Hour, 93411)
	b := crossFinancialFixture(t, Short, "policy-conflict-b", 3422, 3*time.Hour, 93411)
	if _, err := InspectExternalModelAttributionPolicy(context.Background(), []ExternalFinancialEventSource{a, b}); err == nil {
		t.Fatal("same funding transaction used in two incidents")
	}
	c := crossFinancialFixture(t, Short, "policy-collision", 3433, 3*time.Hour, 93433, 3411)
	if _, err := InspectExternalModelAttributionPolicy(context.Background(), []ExternalFinancialEventSource{a, c}); err == nil {
		t.Fatal("same trade IDs counted twice in model fees")
	}
	original := c
	original.Incident.Checksum = "0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := InspectExternalModelAttributionPolicy(context.Background(), []ExternalFinancialEventSource{a, original}); err == nil {
		t.Fatal("invalid source checksum accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectExternalModelAttributionPolicy(ctx, []ExternalFinancialEventSource{a, c}); err == nil {
		t.Fatal("cancelled inspection accepted")
	}
}

func TestExternalModelAttributionPolicyRejectsRehashedClaims(t *testing.T) {
	a := crossFinancialFixture(t, Long, "policy-rehash-a", 3511, 5*time.Hour, 93511)
	b := crossFinancialFixture(t, Short, "policy-rehash-b", 3522, 3*time.Hour, 93522)
	verified, err := InspectExternalModelAttributionPolicy(context.Background(), []ExternalFinancialEventSource{a, b})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"model_fee", "model_gross", "incident_fee", "incident_net", "pretend_funding_zero", "pretend_total_funding_zero", "pretend_final_net", "settlement", "blockers", "funding_id_duplicate"} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(verified)
			var r ExternalModelAttributionPolicy
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			zero := "0"
			switch name {
			case "model_fee":
				r.ProvisionalModels[0].EntryCommissionUSDT = "0.01"
			case "model_gross":
				r.ProvisionalModels[0].ExchangeRealizedUSDT = "3"
			case "incident_fee":
				r.Incidents[0].ExitCommissionUSDT = "9"
			case "incident_net":
				r.Incidents[0].ProvisionalExFundingUSDT = "0"
			case "pretend_funding_zero":
				r.ProvisionalModels[0].ModelFundingUSDT = &zero
			case "pretend_total_funding_zero":
				r.AccountWideFundingUSDT = &zero
			case "pretend_final_net":
				r.ProvisionalModels[0].ModelFinalNetUSDT = &zero
			case "settlement":
				r.SettlementAuthorized = true
			case "blockers":
				r.BlockerCodes = r.BlockerCodes[1:]
			case "funding_id_duplicate":
				r.Incidents[1].FundingTransactionIDs[0] = r.Incidents[0].FundingTransactionIDs[0]
			}
			r.DigestSHA256 = r.digest()
			if err := ValidateExternalModelAttributionPolicy(r); err == nil {
				t.Fatal("rewritten and rehashed false policy accepted")
			}
		})
	}
}

func TestExternalModelAttributionPolicySeparatesDifferentModelLabels(t *testing.T) {
	a := crossFinancialFixtureForModel(t, Long, "multi-model-a", 3611, 5*time.Hour, 0, "ema_rsi_macd_v1")
	b := crossFinancialFixtureForModel(t, Short, "multi-model-b", 3622, 3*time.Hour, 0, "ema_rsi_macd_v2")
	r, err := InspectExternalModelAttributionPolicy(context.Background(), []ExternalFinancialEventSource{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ProvisionalModels) != 2 || r.ProvisionalModels[0].ModelID != "ema_rsi_macd_v1" ||
		r.ProvisionalModels[1].ModelID != "ema_rsi_macd_v2" {
		t.Fatalf("model identities mixed: %+v", r.ProvisionalModels)
	}
	for _, m := range r.ProvisionalModels {
		if len(m.IncidentIDs) != 1 || m.EntryCommissionUSDT != "0.03" || m.ExitCommissionUSDT != "0.025" ||
			m.ExchangeRealizedUSDT != "1.25" || m.ProvisionalExFundingUSDT != "1.195" ||
			m.ModelFundingUSDT != nil || m.ModelFinalNetUSDT != nil {
			t.Fatalf("model attribution incorrect or over-authorized: %+v", m)
		}
	}
	if err := ValidateExternalModelAttributionPolicy(r); err != nil {
		t.Fatal(err)
	}
}
