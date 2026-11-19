package trading

// External-close recovery rehearsal is deliberately NON-MUTATING. It builds a
// deterministic, locally hash-bound operator preview from independent signed
// Demo GET evidence. The hash is NOT a Binance signature, operator approval,
// or a permit to modify the ownership/lifecycle ledger or submit orders.
// A future writer needs a separate, operator-attested, crash-safe protocol.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"sort"

	"binance_trader/internal/exchange"
)

const (
	ExternalCloseRehearsed        = "RECONCILIATION_REHEARSAL_REVIEW_ONLY"
	ExternalCloseRehearsalBlocked = "RECONCILIATION_REHEARSAL_BLOCKED"
)

type ExternalCloseRehearsal struct {
	Schema                         int          `json:"schema"`
	Status                         string       `json:"status"`
	EntryClientOrderID             string       `json:"entry_client_order_id"`
	ModelID                        string       `json:"model_id,omitempty"`
	PositionSide                   PositionSide `json:"position_side"`
	EntryOrderID                   int64        `json:"signed_entry_order_id,omitempty"`
	ExitOrderID                    int64        `json:"signed_external_exit_order_id,omitempty"`
	EntryTradeIDs                  []int64      `json:"signed_entry_trade_ids"`
	ExitTradeIDs                   []int64      `json:"signed_exit_trade_ids"`
	EntryQuantity                  string       `json:"entry_quantity,omitempty"`
	ExitQuantity                   string       `json:"exit_quantity,omitempty"`
	SimulatedRemainingQty          string       `json:"simulated_remaining_quantity,omitempty"`
	SignedEntryCommission          string       `json:"signed_entry_commission,omitempty"`
	SignedExitCommission           string       `json:"signed_exit_commission,omitempty"`
	SignedRealizedPnL              string       `json:"signed_exit_realized_pnl,omitempty"`
	ProvisionalNetExcludingFunding string       `json:"provisional_net_excluding_funding,omitempty"`
	CommissionAsset                string       `json:"commission_asset,omitempty"`
	LocalEvidenceSHA256            string       `json:"local_evidence_sha256,omitempty"`
	SignedReadDigestSHA256         string       `json:"signed_read_digest_sha256,omitempty"`
	PreviewSHA256                  string       `json:"preview_sha256,omitempty"`
	Blockers                       []string     `json:"blockers"`
	RequiredBeforeMutation         []string     `json:"required_before_mutation"`
	ManualReviewRequired           bool         `json:"manual_review_required"`
	LocalLifecycleChanged          bool         `json:"local_lifecycle_changed"` // ALWAYS false
	LocalOwnershipChanged          bool         `json:"local_ownership_changed"` // ALWAYS false
	SettlementAuthorized           bool         `json:"settlement_authorized"`   // ALWAYS false
	TradingAuthorized              bool         `json:"trading_authorized"`      // ALWAYS false
}

// fingerprintRehearsal seals the preview with local SHA-256 so accidental
// modification is detectable. It does not authenticate an external JSON file:
// only ReviewSignedLedgerHistory with signed Demo GETs can produce provenance.
func fingerprintRehearsal(p ExternalCloseRehearsal) string {
	p.PreviewSHA256 = ""
	b, _ := json.Marshal(p)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func VerifyExternalCloseRehearsalChecksum(p ExternalCloseRehearsal) bool {
	return p.Schema == 1 && len(p.PreviewSHA256) == 64 && p.PreviewSHA256 == fingerprintRehearsal(p) &&
		!p.SettlementAuthorized && !p.TradingAuthorized && !p.LocalLifecycleChanged && !p.LocalOwnershipChanged && p.ManualReviewRequired
}

type signedRehearsalBinding struct {
	Schema              int
	LocalEvidenceSHA256 string
	Entry               SignedLedgerOrder
	Exit                SignedExternalOrder
	Case                ExternalCloseRecoveryCase
	EntryTrades         []exchange.AccountTrade
	ExitTrades          []exchange.AccountTrade
	Account             exchange.Snapshot
}

func signedRehearsalDigest(b signedRehearsalBinding) string {
	sort.Slice(b.EntryTrades, func(i, j int) bool { return b.EntryTrades[i].TradeID < b.EntryTrades[j].TradeID })
	sort.Slice(b.ExitTrades, func(i, j int) bool { return b.ExitTrades[i].TradeID < b.ExitTrades[j].TradeID })
	raw, _ := json.Marshal(b)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func buildExternalCloseRehearsals(report SignedLedgerHistoryReport, localSHA string, remoteTrades map[int64]exchange.AccountTrade, externalByOrder map[int64][]exchange.AccountTrade, account exchange.Snapshot) []ExternalCloseRehearsal {
	result := make([]ExternalCloseRehearsal, 0, len(report.ExternalCloseCases))
	for _, c := range report.ExternalCloseCases {
		p := ExternalCloseRehearsal{
			Schema: 1, Status: ExternalCloseRehearsalBlocked, EntryClientOrderID: c.EntryClientOrderID,
			ModelID: c.ModelID, PositionSide: c.PositionSide,
			EntryOrderID: c.EntryOrderID, ExitOrderID: c.ExitOrderID,
			EntryTradeIDs: []int64{}, ExitTradeIDs: []int64{}, Blockers: []string{},
			RequiredBeforeMutation: []string{
				"Operator must independently inspect complete signed trade, order and funding history outside the bounded review window",
				"Design and approve a new immutable operator-attested external-close journal; do not rewrite the original entry or MAY_HAVE_SENT event",
				"Prove crash/restart exactly-once fill import and escrow reservation handling with independent reconciliation",
				"Independently verify commission assets, funding, net PnL and other instruments before any settlement",
				"Retain order/auto-trading locks until separate authenticated end-to-end Demo validation",
			},
			ManualReviewRequired: true,
		}
		block := func(s string) { p.Blockers = append(p.Blockers, s) }
		if c.Status != ExternalCloseCandidate || !report.HistoryWindowComplete || !report.SignedAccountFresh || len(report.Orders) != 1 || len(report.ExternalOrders) != 1 {
			block("independent signed candidate, complete bounded history, and fresh Demo snapshot required")
		} else if !c.LocalEntryFillsMatched || !report.LocalFillsMatchSigned {
			block("locally imported signed entry fills are missing; recover entry through the existing idempotent signed order path first")
		} else if c.HistoricalCoverageLimited == false {
			// This is intentionally still only a rehearsal even when an audit
			// claims to cover more history. Never permit forged completeness.
			block("unexpected historical scope metadata")
		} else {
			entry, exit := report.Orders[0], report.ExternalOrders[0]
			if entry.ClientOrderID != c.EntryClientOrderID || entry.SignedOrderID != c.EntryOrderID || !entry.ExactFillsMatched || entry.Status != "FILLED" ||
				exit.OrderID != c.ExitOrderID || exit.Status != "FILLED" || !exit.VerifiedOrderIdentity || account.Environment != "demo" {
				block("entry/exit signed order identity mismatch")
			} else {
				entryFills := make([]exchange.AccountTrade, 0)
				for _, t := range remoteTrades {
					if t.OrderID == entry.SignedOrderID {
						entryFills = append(entryFills, t)
					}
				}
				exitFills := append([]exchange.AccountTrade{}, externalByOrder[exit.OrderID]...)
				entryQty, exitQty, entryFee, exitFee, realized := new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat), new(big.Rat)
				valid := len(entryFills) == entry.SignedTradeCount && len(exitFills) == exit.TradeCount && len(entryFills) > 0 && len(exitFills) > 0
				unique := map[int64]bool{}
				for _, bucket := range []struct {
					fills    []exchange.AccountTrade
					qty, fee *big.Rat
					entering bool
				}{
					{entryFills, entryQty, entryFee, true}, {exitFills, exitQty, exitFee, false},
				} {
					for _, t := range bucket.fills {
						q, qe := positiveRat(t.Quantity)
						f, fe := signedRat(t.Commission)
						realizedPart, re := signedRat(t.RealizedPnL)
						if qe != nil || fe != nil || re != nil || f.Sign() < 0 || t.CommissionAsset != "USDT" ||
							t.TradeID <= 0 || unique[t.TradeID] || t.Symbol != "BTCUSDT" || t.OrderID <= 0 {
							valid = false
							continue
						}
						unique[t.TradeID] = true
						bucket.qty.Add(bucket.qty, q)
						bucket.fee.Add(bucket.fee, f)
						if bucket.entering {
							if realizedPart.Sign() != 0 {
								valid = false
							}
							p.EntryTradeIDs = append(p.EntryTradeIDs, t.TradeID)
						} else {
							realized.Add(realized, realizedPart)
							p.ExitTradeIDs = append(p.ExitTradeIDs, t.TradeID)
						}
					}
				}
				expected, err := positiveRat(c.EntryQuantity)
				if err != nil || !valid || entryQty.Cmp(expected) != 0 || exitQty.Cmp(expected) != 0 ||
					formatOwnershipDecimal(realized) != c.SignedRealizedPnL || formatOwnershipDecimal(exitFee) != c.SignedExitCommission {
					block("in-memory replay quantity, trade IDs, PnL, or commission could not be verified exactly")
				} else {
					sort.Slice(p.EntryTradeIDs, func(i, j int) bool { return p.EntryTradeIDs[i] < p.EntryTradeIDs[j] })
					sort.Slice(p.ExitTradeIDs, func(i, j int) bool { return p.ExitTradeIDs[i] < p.ExitTradeIDs[j] })
					p.EntryQuantity, p.ExitQuantity = formatOwnershipDecimal(entryQty), formatOwnershipDecimal(exitQty)
					p.SimulatedRemainingQty = formatOwnershipDecimal(new(big.Rat).Sub(entryQty, exitQty))
					p.SignedEntryCommission, p.SignedExitCommission = formatOwnershipDecimal(entryFee), formatOwnershipDecimal(exitFee)
					p.SignedRealizedPnL = formatOwnershipDecimal(realized)
					net := new(big.Rat).Sub(realized, entryFee)
					net.Sub(net, exitFee)
					p.ProvisionalNetExcludingFunding = formatOwnershipDecimal(net)
					p.CommissionAsset = "USDT"
					p.LocalEvidenceSHA256 = localSHA
					p.SignedReadDigestSHA256 = signedRehearsalDigest(signedRehearsalBinding{1, localSHA, entry, exit, c, entryFills, exitFills, account})
					p.Status = ExternalCloseRehearsed
					// A quantity-matched shadow replay NEVER resolves funding,
					// historical attribution, external intent or account ownership.
					if p.SimulatedRemainingQty != "0" {
						p.Status = ExternalCloseRehearsalBlocked
						block("nonzero simulated remaining quantity; replay invalid")
					}
					block("dry-run only: funding, earlier history, external attribution, operator approval and crash-safe writes remain unresolved")
				}
			}
		}
		p.PreviewSHA256 = fingerprintRehearsal(p)
		result = append(result, p)
	}
	return result
}
