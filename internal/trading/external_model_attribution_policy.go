package trading

// dev.3.133 — conservative, OFFLINE model-cost attribution policy.  A model
// label stored on a locally reviewed entry is a BOUNDED observation, not a
// proof of account-wide ownership.  Exchange FUNDING_FEE is account-level:
// without independently complete position history it cannot be attributed
// to a specific model (even if the observed amount is zero).  This inspector
// NEVER writes ledgers, establishes settlement authority or calls Binance.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
)

const ExternalModelAttributionPolicyNotPosted = "BOUNDED_MODEL_COST_GROUPING_FUNDING_UNALLOCATED_NOT_POSTED"

type ExternalModelAttributionIncident struct {
	ClientOrderID            string       `json:"client_order_id"`
	ModelID                  string       `json:"locally_recorded_model_id"`
	PositionSide             PositionSide `json:"position_side"`
	ModelEvidenceSHA256      string       `json:"model_evidence_sha256"`
	EntryCommissionUSDT      string       `json:"bounded_entry_commission_usdt"`
	ExitCommissionUSDT       string       `json:"bounded_exit_commission_usdt"`
	RealizedUSDT             string       `json:"bounded_exchange_realized_usdt"`
	ProvisionalExFundingUSDT string       `json:"bounded_provisional_net_ex_funding_usdt"`
	UnallocatedFundingUSDT   string       `json:"bounded_account_funding_unallocated_usdt"`
	FundingTransactionIDs    []int64      `json:"observed_funding_transaction_ids"`
}

// A grouped amount is only a sum of independently rechecked bounded rows.
// The group name is not an attestation of the trading model's real position.
// Nil funding/final net is mandatory.  Zero would mean an unjustified claim.
type ExternalProvisionalModelCostGroup struct {
	ModelID                  string   `json:"locally_recorded_model_id"`
	IncidentIDs              []string `json:"incident_ids"`
	EntryCommissionUSDT      string   `json:"bounded_entry_commission_usdt"`
	ExitCommissionUSDT       string   `json:"bounded_exit_commission_usdt"`
	ExchangeRealizedUSDT     string   `json:"bounded_exchange_realized_usdt"`
	ProvisionalExFundingUSDT string   `json:"bounded_provisional_net_ex_funding_usdt"`
	ModelFundingUSDT         *string  `json:"model_funding_usdt"`
	ModelFinalNetUSDT        *string  `json:"model_final_net_usdt"`
}

type ExternalModelAttributionPolicy struct {
	Schema                int                                 `json:"schema"`
	Stage                 string                              `json:"stage"`
	CrossEventAuditSHA256 string                              `json:"cross_event_audit_sha256"`
	Incidents             []ExternalModelAttributionIncident  `json:"incidents"`
	ProvisionalModels     []ExternalProvisionalModelCostGroup `json:"provisional_model_groups"`
	// Account-level rows are NEVER summed across separate offline incidents:
	// neither matching local IDs nor disjoint timestamps attest one account.
	AccountWideFundingUSDT *string  `json:"account_wide_funding_usdt"`
	CompleteHistoryProven  bool     `json:"complete_history_proven"`
	SharedAccountProven    bool     `json:"shared_account_proven"`
	ModelOwnershipProven   bool     `json:"model_ownership_proven"`
	FundingAllocated       bool     `json:"funding_allocated"`
	PostingApplied         bool     `json:"posting_applied"`
	SettlementAuthorized   bool     `json:"settlement_authorized"`
	TradingAuthorized      bool     `json:"trading_authorized"`
	BlockerCodes           []string `json:"blocker_codes"`
	DigestSHA256           string   `json:"digest_sha256"`
}

func (r ExternalModelAttributionPolicy) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

func externalModelPolicyBlockers() []string {
	return []string{
		"SAME_ACCOUNT_NOT_INDEPENDENTLY_ATTESTED",
		"COMPLETE_RELEVANT_ACCOUNT_HISTORY_UNPROVEN",
		"MODEL_OWNERSHIP_NOT_INDEPENDENTLY_PROVEN",
		"ACCOUNT_FUNDING_NOT_MODEL_ALLOCATABLE",
		"ACTUAL_MODEL_NET_PNL_UNPROVEN",
		"REAL_FINANCIAL_COMMIT_NOT_AUTHORIZED",
		"HTTP_423_ORDER_LOCK_REQUIRED",
	}
}

// ValidateExternalModelAttributionPolicy checks the structure and mathematical
// invariants of a serialized local report. A recomputed SHA-256 cannot attest
// the source — ONLY the Inspect function below independently re-reads sources.
func ValidateExternalModelAttributionPolicy(r ExternalModelAttributionPolicy) error {
	if r.Schema != 1 || r.Stage != ExternalModelAttributionPolicyNotPosted ||
		!externalReviewHash.MatchString(r.CrossEventAuditSHA256) ||
		len(r.Incidents) < 2 || len(r.Incidents) > 64 || len(r.ProvisionalModels) == 0 ||
		r.AccountWideFundingUSDT != nil || r.CompleteHistoryProven || r.SharedAccountProven ||
		r.ModelOwnershipProven || r.FundingAllocated || r.PostingApplied ||
		r.SettlementAuthorized || r.TradingAuthorized || r.DigestSHA256 != r.digest() {
		return errors.New("invalid or authorizing bounded model allocation policy")
	}
	blockers := externalModelPolicyBlockers()
	if len(r.BlockerCodes) != len(blockers) {
		return errors.New("missing mandatory model allocation blockers")
	}
	for i, v := range blockers {
		if r.BlockerCodes[i] != v {
			return errors.New("model allocation blocker altered")
		}
	}
	type totals struct {
		entry, exit, realized *big.Rat
		ids                   []string
	}
	byModel := map[string]*totals{}
	lastIncident := ""
	seenFunding := make(map[int64]bool)
	for _, ev := range r.Incidents {
		if !externalReviewID.MatchString(ev.ClientOrderID) || ev.ClientOrderID <= lastIncident || ev.ModelID == "" ||
			(ev.PositionSide != Long && ev.PositionSide != Short) || !externalReviewHash.MatchString(ev.ModelEvidenceSHA256) {
			return errors.New("unordered/invalid model incident identity")
		}
		lastIncident = ev.ClientOrderID
		entry, err := signedRat(ev.EntryCommissionUSDT)
		if err != nil || entry.Sign() < 0 {
			return errors.New("invalid entry commission")
		}
		exit, err := signedRat(ev.ExitCommissionUSDT)
		if err != nil || exit.Sign() < 0 {
			return errors.New("invalid exit commission")
		}
		realized, err := signedRat(ev.RealizedUSDT)
		if err != nil {
			return errors.New("invalid realized pnl")
		}
		net, err := signedRat(ev.ProvisionalExFundingUSDT)
		if err != nil {
			return errors.New("invalid provisional net")
		}
		funding, err := signedRat(ev.UnallocatedFundingUSDT)
		if err != nil {
			return errors.New("invalid unallocated funding")
		}
		if err := ensureExactModelFinancialDecimal(entry); err != nil {
			return err
		}
		if err := ensureExactModelFinancialDecimal(exit); err != nil {
			return err
		}
		if err := ensureExactModelFinancialDecimal(realized); err != nil {
			return err
		}
		if err := ensureExactModelFinancialDecimal(net); err != nil {
			return err
		}
		if err := ensureExactModelFinancialDecimal(funding); err != nil {
			return err
		}
		if ev.EntryCommissionUSDT != formatOwnershipDecimal(entry) || ev.ExitCommissionUSDT != formatOwnershipDecimal(exit) ||
			ev.RealizedUSDT != formatOwnershipDecimal(realized) || ev.ProvisionalExFundingUSDT != formatOwnershipDecimal(net) ||
			ev.UnallocatedFundingUSDT != formatOwnershipDecimal(funding) ||
			new(big.Rat).Sub(new(big.Rat).Sub(realized, entry), exit).Cmp(net) != 0 {
			return errors.New("per-incident money invariant failed")
		}
		lastFunding := int64(0)
		for _, id := range ev.FundingTransactionIDs {
			if id <= lastFunding {
				return errors.New("invalid or duplicated incident funding transaction")
			}
			if seenFunding[id] {
				return errors.New("funding transaction repeated across incidents")
			}
			seenFunding[id] = true
			lastFunding = id
		}
		m := byModel[ev.ModelID]
		if m == nil {
			m = &totals{entry: new(big.Rat), exit: new(big.Rat), realized: new(big.Rat)}
			byModel[ev.ModelID] = m
		}
		m.ids = append(m.ids, ev.ClientOrderID)
		m.entry.Add(m.entry, entry)
		m.exit.Add(m.exit, exit)
		m.realized.Add(m.realized, realized)
	}
	if len(r.ProvisionalModels) != len(byModel) {
		return errors.New("model grouping count differs from incidents")
	}
	lastModel := ""
	for _, m := range r.ProvisionalModels {
		if m.ModelID <= lastModel || m.ModelFundingUSDT != nil || m.ModelFinalNetUSDT != nil {
			return errors.New("unordered model or unproven funding allocation")
		}
		lastModel = m.ModelID
		v := byModel[m.ModelID]
		if v == nil || len(m.IncidentIDs) != len(v.ids) {
			return errors.New("unexpected model group")
		}
		for i, id := range v.ids {
			if m.IncidentIDs[i] != id {
				return errors.New("model incident reference changed")
			}
		}
		sum := new(big.Rat).Sub(new(big.Rat).Sub(new(big.Rat).Set(v.realized), v.entry), v.exit)
		for _, x := range []*big.Rat{v.entry, v.exit, v.realized, sum} {
			if err := ensureExactModelFinancialDecimal(x); err != nil {
				return err
			}
		}
		if m.EntryCommissionUSDT != formatOwnershipDecimal(v.entry) ||
			m.ExitCommissionUSDT != formatOwnershipDecimal(v.exit) ||
			m.ExchangeRealizedUSDT != formatOwnershipDecimal(v.realized) ||
			m.ProvisionalExFundingUSDT != formatOwnershipDecimal(sum) {
			return errors.New("model fee/realized/provisional totals inconsistent")
		}
	}
	return nil
}

// InspectExternalModelAttributionPolicy is the only source-aware construction
// path. It first performs existing cross-event identity/ownership checks, then
// re-derives EVERY model fee and funding row from the recorded incident and
// local source journal, and re-reads once more to catch drift.  A cross-event
// trade, order, funding ID or Hedge overlap conflict is a hard error, not a
// sum that is allowed to silently double charge a model.
func InspectExternalModelAttributionPolicy(ctx context.Context, events []ExternalFinancialEventSource) (ExternalModelAttributionPolicy, error) {
	var empty ExternalModelAttributionPolicy
	audit, err := InspectExternalCrossEventFinancials(ctx, events)
	if err != nil {
		return empty, err
	}
	if audit.Stage != ExternalCrossEventNoConflict || len(audit.Conflicts) != 0 ||
		audit.DigestSHA256 != audit.digest() || audit.SharedAccountProven ||
		audit.CompleteHistoryProven || audit.FundingAllocated || audit.PostingApplied ||
		audit.SettlementAuthorized || audit.TradingAuthorized {
		return empty, errors.New("model cost grouping blocked by cross-event conflict or authority claim")
	}
	ordered := append([]ExternalFinancialEventSource(nil), events...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ClientOrderID < ordered[j].ClientOrderID })
	incidents := make([]ExternalModelAttributionIncident, 0, len(ordered))
	byModel := map[string]*ExternalProvisionalModelCostGroup{}
	for _, e := range ordered {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		r, err := InspectExternalModelFinancialEvidence(ctx, e.DataDir, e.ClientOrderID, e.Incident)
		if err != nil {
			return empty, fmt.Errorf("incident %s fee trace: %w", e.ClientOrderID, err)
		}
		if err := ValidateExternalModelFinancialEvidence(r); err != nil {
			return empty, err
		}
		ids := make([]int64, 0, len(r.FundingRows))
		for _, row := range r.FundingRows {
			ids = append(ids, row.TranID)
		}
		incidents = append(incidents, ExternalModelAttributionIncident{
			ClientOrderID: r.ClientOrderID, ModelID: r.ModelID, PositionSide: r.PositionSide,
			ModelEvidenceSHA256: r.DigestSHA256, EntryCommissionUSDT: r.EntryCommissionUSDT,
			ExitCommissionUSDT: r.ExitCommissionUSDT, RealizedUSDT: r.ExchangeExitRealizedUSDT,
			ProvisionalExFundingUSDT: r.ProvisionalExFundingUSDT,
			UnallocatedFundingUSDT:   r.BoundedAccountFundingUSDT, FundingTransactionIDs: ids,
		})
		m := byModel[r.ModelID]
		if m == nil {
			m = &ExternalProvisionalModelCostGroup{ModelID: r.ModelID}
			byModel[r.ModelID] = m
		}
		m.IncidentIDs = append(m.IncidentIDs, r.ClientOrderID)
	}
	models := make([]ExternalProvisionalModelCostGroup, 0, len(byModel))
	for _, m := range byModel {
		entry, exit, realized := new(big.Rat), new(big.Rat), new(big.Rat)
		for _, ev := range incidents {
			if ev.ModelID != m.ModelID {
				continue
			}
			a, _ := signedRat(ev.EntryCommissionUSDT)
			b, _ := signedRat(ev.ExitCommissionUSDT)
			c, _ := signedRat(ev.RealizedUSDT)
			entry.Add(entry, a)
			exit.Add(exit, b)
			realized.Add(realized, c)
		}
		net := new(big.Rat).Sub(new(big.Rat).Sub(new(big.Rat).Set(realized), entry), exit)
		for _, amount := range []*big.Rat{entry, exit, realized, net} {
			if err := ensureExactModelFinancialDecimal(amount); err != nil {
				return empty, err
			}
		}
		m.EntryCommissionUSDT = formatOwnershipDecimal(entry)
		m.ExitCommissionUSDT = formatOwnershipDecimal(exit)
		m.ExchangeRealizedUSDT = formatOwnershipDecimal(realized)
		m.ProvisionalExFundingUSDT = formatOwnershipDecimal(net)
		models = append(models, *m)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ModelID < models[j].ModelID })
	out := ExternalModelAttributionPolicy{
		Schema: 1, Stage: ExternalModelAttributionPolicyNotPosted,
		CrossEventAuditSHA256: audit.DigestSHA256, Incidents: incidents,
		ProvisionalModels: models, BlockerCodes: externalModelPolicyBlockers(),
	}
	out.DigestSHA256 = out.digest()
	if err := ValidateExternalModelAttributionPolicy(out); err != nil {
		return empty, err
	}
	// Pinned on-disk sources are checked again after construction. A local
	// fingerprint is never substituted for independent exchange attestation.
	fresh, err := InspectExternalCrossEventFinancials(ctx, events)
	if err != nil || fresh.DigestSHA256 != audit.DigestSHA256 {
		return empty, errors.New("cross-event evidence drifted during allocation audit")
	}
	for i, e := range ordered {
		r, err := InspectExternalModelFinancialEvidence(ctx, e.DataDir, e.ClientOrderID, e.Incident)
		if err != nil || r.DigestSHA256 != incidents[i].ModelEvidenceSHA256 {
			return empty, errors.New("model financial evidence drifted during allocation audit")
		}
	}
	return out, nil
}
