package trading

// Read-only cross-incident accounting review. Each incident is independently
// revalidated against its own STOPPED local data snapshot and bounded signed
// GET archive. Neither local hashes nor a manifest attest a shared Binance
// account, complete history, model-level funding, or settlement authority.
// This never changes the single-global-proposal writer's no-double-allocation
// invariant. Identical IDs across different snapshots are conservatively
// treated as conflicts until the operator proves account provenance.
import (
	"context"
	"errors"
	"fmt"
	"sort"
)

const (
	ExternalCrossEventNoConflict = "CROSS_EVENT_NO_LOCAL_CONFLICT_OBSERVED_NOT_POSTED"
	ExternalCrossEventConflict   = "CROSS_EVENT_CONFLICTS_FOUND_NOT_POSTED"
)

type ExternalFinancialEventSource struct {
	DataDir       string                  `json:"data"`
	ClientOrderID string                  `json:"id"`
	Incident      PackageAIncidentArchive `json:"-"`
}

type ExternalCrossEventMember struct {
	ClientOrderID               string       `json:"client_order_id"`
	ModelID                     string       `json:"model_id"`
	PositionSide                PositionSide `json:"position_side"`
	EntryOrderID                int64        `json:"entry_order_id"`
	ExitOrderID                 int64        `json:"exit_order_id"`
	FirstEntryMS                int64        `json:"first_entry_ms"`
	LastExitMS                  int64        `json:"last_exit_ms"`
	BoundedFinancialSHA256      string       `json:"bounded_financial_sha256"`
	OriginalEvidenceSHA256      string       `json:"original_evidence_sha256"`
	ProvisionalNetExFundingUSDT string       `json:"provisional_net_ex_funding_usdt"`
	UnallocatedFundingUSDT      string       `json:"unallocated_funding_usdt"`
}

type ExternalCrossEventConflictRecord struct {
	Code           string `json:"code"`
	ClientOrderID1 string `json:"client_order_id_1"`
	ClientOrderID2 string `json:"client_order_id_2"`
	EvidenceID     int64  `json:"evidence_id,omitempty"`
}

type ExternalCrossEventFinancialAudit struct {
	Schema    int                                `json:"schema"`
	Stage     string                             `json:"stage"`
	Members   []ExternalCrossEventMember         `json:"members"`
	Conflicts []ExternalCrossEventConflictRecord `json:"conflicts"`
	// A zero-conflict bounded scan never proves global ownership or payment.
	BlockerCodes          []string `json:"blocker_codes"`
	SharedAccountProven   bool     `json:"shared_account_proven"`
	CompleteHistoryProven bool     `json:"complete_history_proven"`
	FundingAllocated      bool     `json:"funding_allocated"`
	PostingApplied        bool     `json:"posting_applied"`
	OwnershipChanged      bool     `json:"ownership_changed"`
	LifecycleChanged      bool     `json:"lifecycle_changed"`
	SettlementAuthorized  bool     `json:"settlement_authorized"`
	TradingAuthorized     bool     `json:"trading_authorized"`
	DigestSHA256          string   `json:"digest_sha256"`
}

func (r ExternalCrossEventFinancialAudit) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

type externalCrossEventChecked struct {
	member         ExternalCrossEventMember
	entryTradeIDs  []int64
	exitTradeIDs   []int64
	fundingTranIDs []int64
}

// InspectExternalCrossEventFinancials compares 2..64 incident snapshots. A
// caller cannot submit bare self-asserted reconciliation digests: every member
// is reconstructed and checked with the v3.122 bounded inspector first.
// Source errors abort inspection; evidence conflicts produce a NOT-POSTED
// report for operator review. For conflict reports the CLI exits nonzero.
func InspectExternalCrossEventFinancials(ctx context.Context, events []ExternalFinancialEventSource) (ExternalCrossEventFinancialAudit, error) {
	var zero ExternalCrossEventFinancialAudit
	if ctx == nil || len(events) < 2 || len(events) > 64 {
		return zero, errors.New("2..64 captured external-close incidents required")
	}
	seenID := make(map[string]bool, len(events))
	seenData := make(map[string]bool, len(events))
	checked := make([]externalCrossEventChecked, 0, len(events))
	for _, e := range events {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if e.DataDir == "" || !externalReviewID.MatchString(e.ClientOrderID) || seenID[e.ClientOrderID] || seenData[e.DataDir] {
			return zero, errors.New("missing, repeated or invalid incident ID/data root")
		}
		seenID[e.ClientOrderID], seenData[e.DataDir] = true, true
		result, err := InspectExternalCloseBoundedFinancials(ctx, e.DataDir, e.ClientOrderID, e.Incident)
		if err != nil {
			return zero, fmt.Errorf("incident %s: %w", e.ClientOrderID, err)
		}
		if result.ClientOrderID != e.ClientOrderID || result.ProposalSHA256 == "" ||
			result.DigestSHA256 != result.digest() || result.Stage != ExternalBoundedFinancialReconciledNotPosted ||
			result.PostingApplied || result.SettlementAuthorized || result.TradingAuthorized || result.OwnershipChanged || result.LifecycleChanged {
			return zero, errors.New("inconsistent or authorizing bounded financial inspection")
		}
		if len(e.Incident.Close.EntryFills) == 0 || len(e.Incident.Close.ExitFills) == 0 ||
			e.Incident.Close.HistoryFromMS <= 0 || e.Incident.Close.HistoryToMS < e.Incident.Close.HistoryFromMS {
			return zero, errors.New("missing verified entry/exit chronology")
		}
		p, err := NewExternalCloseSettlementProposalStore(e.DataDir)
		if err != nil {
			return zero, err
		}
		proposal, err := p.Load(e.ClientOrderID)
		if err != nil {
			return zero, err
		}
		if proposal.Checksum != result.ProposalSHA256 {
			return zero, errors.New("proposal changed during cross-event inspection")
		}
		member := ExternalCrossEventMember{
			ClientOrderID: e.ClientOrderID, ModelID: result.ModelID, PositionSide: result.PositionSide,
			EntryOrderID: proposal.EntryOrderID, ExitOrderID: proposal.ExitOrderID,
			FirstEntryMS: e.Incident.Close.HistoryFromMS, LastExitMS: e.Incident.Close.HistoryToMS,
			BoundedFinancialSHA256: result.DigestSHA256, OriginalEvidenceSHA256: result.RealEvidenceSHA256,
			ProvisionalNetExFundingUSDT: result.ProvisionalNetExFundingUSDT,
			UnallocatedFundingUSDT:      result.UnallocatedFundingUSDT,
		}
		fIDs := make([]int64, 0, len(e.Incident.Close.FundingRows))
		for _, row := range e.Incident.Close.FundingRows {
			fIDs = append(fIDs, row.TranID)
		}
		checked = append(checked, externalCrossEventChecked{
			member: member, entryTradeIDs: append([]int64(nil), result.EntryTradeIDs...),
			exitTradeIDs: append([]int64(nil), result.ExitTradeIDs...), fundingTranIDs: fIDs,
		})
	}
	// Sort and compare all freshly inspected members.
	sort.Slice(checked, func(i, j int) bool { return checked[i].member.ClientOrderID < checked[j].member.ClientOrderID })
	unique := crossEventConflicts(checked)
	members := make([]ExternalCrossEventMember, 0, len(checked))
	for _, c := range checked {
		members = append(members, c.member)
	}
	stage := ExternalCrossEventNoConflict
	if len(unique) != 0 {
		stage = ExternalCrossEventConflict
	}
	out := ExternalCrossEventFinancialAudit{
		Schema: 1, Stage: stage, Members: members, Conflicts: unique,
		BlockerCodes: []string{
			"SAME_DEMO_ACCOUNT_NOT_INDEPENDENTLY_ATTESTED",
			"COMPLETE_RELEVANT_ACCOUNT_HISTORY_UNPROVEN",
			"POOLED_SIDE_OWNERSHIP_AND_FUNDING_UNPROVEN",
			"ATOMIC_CROSS_LEDGER_POSTING_NOT_IMPLEMENTED",
			"INDEPENDENT_SETTLEMENT_APPROVAL_REQUIRED",
			"HTTP_423_ORDER_LOCK_REQUIRED",
		},
	}
	out.DigestSHA256 = out.digest()
	// No concurrent writers are allowed. Revalidate every member after the
	// cross comparison, so an earlier snapshot changed during later reads is
	// not reported as a stable result.
	for _, e := range events {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		fresh, err := InspectExternalCloseBoundedFinancials(ctx, e.DataDir, e.ClientOrderID, e.Incident)
		if err != nil {
			return zero, fmt.Errorf("cross-event evidence changed for %s: %w", e.ClientOrderID, err)
		}
		old := ""
		for _, m := range members {
			if m.ClientOrderID == e.ClientOrderID {
				old = m.BoundedFinancialSHA256
				break
			}
		}
		if fresh.DigestSHA256 != old {
			return zero, errors.New("cross-event evidence changed during audit")
		}
	}
	return out, nil
}

// crossEventConflicts operates on validated, evidence-derived identities. Both
// single-run audits and the durable offline reservation protocol reuse it.
func crossEventConflicts(checked []externalCrossEventChecked) []ExternalCrossEventConflictRecord {
	if len(checked) < 2 {
		return []ExternalCrossEventConflictRecord{}
	}
	sort.Slice(checked, func(i, j int) bool { return checked[i].member.ClientOrderID < checked[j].member.ClientOrderID })
	conflicts := []ExternalCrossEventConflictRecord{}
	for i, a := range checked {
		for _, b := range checked[i+1:] {
			add := func(code string, id int64) {
				conflicts = append(conflicts, ExternalCrossEventConflictRecord{Code: code,
					ClientOrderID1: a.member.ClientOrderID, ClientOrderID2: b.member.ClientOrderID, EvidenceID: id})
			}
			if a.member.EntryOrderID == b.member.EntryOrderID || a.member.EntryOrderID == b.member.ExitOrderID ||
				a.member.ExitOrderID == b.member.EntryOrderID || a.member.ExitOrderID == b.member.ExitOrderID {
				// Report each duplicated order identity only once.
				idsA, idsB := []int64{a.member.EntryOrderID, a.member.ExitOrderID}, []int64{b.member.EntryOrderID, b.member.ExitOrderID}
				for _, x := range idsA {
					for _, y := range idsB {
						if x == y {
							add("ORDER_ID_REUSED", x)
						}
					}
				}
			}
			byTrade := make(map[int64]bool, len(a.entryTradeIDs)+len(a.exitTradeIDs))
			for _, id := range a.entryTradeIDs {
				byTrade[id] = true
			}
			for _, id := range a.exitTradeIDs {
				byTrade[id] = true
			}
			for _, id := range append(append([]int64(nil), b.entryTradeIDs...), b.exitTradeIDs...) {
				if byTrade[id] {
					add("TRADE_ID_REUSED", id)
				}
			}
			byFunding := make(map[int64]bool, len(a.fundingTranIDs))
			for _, id := range a.fundingTranIDs {
				byFunding[id] = true
			}
			for _, id := range b.fundingTranIDs {
				if byFunding[id] {
					add("BOUNDED_FUNDING_TX_REUSED_UNALLOCATED", id)
				}
			}
			if a.member.PositionSide == b.member.PositionSide &&
				a.member.FirstEntryMS <= b.member.LastExitMS && b.member.FirstEntryMS <= a.member.LastExitMS {
				add("OVERLAPPING_POOLED_HEDGE_SIDE_OWNERSHIP_UNPROVEN", 0)
			}
		}
	}
	// Dedupe/sort conflicts: a malformed candidate must not amplify results.
	sort.Slice(conflicts, func(i, j int) bool {
		a, b := conflicts[i], conflicts[j]
		if a.ClientOrderID1 != b.ClientOrderID1 {
			return a.ClientOrderID1 < b.ClientOrderID1
		}
		if a.ClientOrderID2 != b.ClientOrderID2 {
			return a.ClientOrderID2 < b.ClientOrderID2
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.EvidenceID < b.EvidenceID
	})
	unique := conflicts[:0]
	for _, c := range conflicts {
		if len(unique) == 0 || unique[len(unique)-1] != c {
			unique = append(unique, c)
		}
	}
	return unique
}
