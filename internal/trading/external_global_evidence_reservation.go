package trading

// The global OFFLINE evidence reservation registry is a simulation-only,
// append-only collision fence. It does NOT allocate real Binance fills, post
// financial transactions, or prove that independent snapshots belong to the
// same account. No production runtime imports or invokes this writer.
//
// The exclusive creation of a sequential slot is the linearization point.
// A torn or missing slot is never repaired automatically. It blocks all later
// reservations until an operator reviews the raw evidence out of band.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const ExternalGlobalEvidenceReserved = "GLOBAL_EVIDENCE_RESERVED_NOT_POSTED"

type ExternalGlobalEvidenceClaim struct {
	ClientOrderID          string       `json:"client_order_id"`
	ModelID                string       `json:"model_id"`
	PositionSide           PositionSide `json:"position_side"`
	EntryOrderID           int64        `json:"entry_order_id"`
	ExitOrderID            int64        `json:"exit_order_id"`
	EntryTradeIDs          []int64      `json:"entry_trade_ids"`
	ExitTradeIDs           []int64      `json:"exit_trade_ids"`
	FundingTranIDs         []int64      `json:"funding_tran_ids"`
	FirstEntryMS           int64        `json:"first_entry_ms"`
	LastExitMS             int64        `json:"last_exit_ms"`
	BoundedFinancialSHA256 string       `json:"bounded_financial_sha256"`
	OriginalEvidenceSHA256 string       `json:"original_evidence_sha256"`
}

type ExternalGlobalEvidenceReservation struct {
	Schema               int                           `json:"schema"`
	Stage                string                        `json:"stage"`
	Sequence             int                           `json:"sequence"`
	PreviousChecksum     string                        `json:"previous_checksum,omitempty"`
	BatchAuditSHA256     string                        `json:"batch_audit_sha256"`
	BatchKey             string                        `json:"batch_key"`
	Claims               []ExternalGlobalEvidenceClaim `json:"claims"`
	PostingApplied       bool                          `json:"posting_applied"`
	SettlementAuthorized bool                          `json:"settlement_authorized"`
	TradingAuthorized    bool                          `json:"trading_authorized"`
	Checksum             string                        `json:"checksum"`
}

func (r ExternalGlobalEvidenceReservation) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

func (r ExternalGlobalEvidenceReservation) key() string {
	return externalCloseHash(struct {
		Audit  string
		Claims []ExternalGlobalEvidenceClaim
	}{r.BatchAuditSHA256, r.Claims})
}

type ExternalGlobalEvidenceReservationState struct {
	Schema                int                                 `json:"schema"`
	Stage                 string                              `json:"stage"`
	Reservations          []ExternalGlobalEvidenceReservation `json:"reservations"`
	TotalClaims           int                                 `json:"total_claims"`
	LastChecksum          string                              `json:"last_checksum,omitempty"`
	SharedAccountProven   bool                                `json:"shared_account_proven"`
	CompleteHistoryProven bool                                `json:"complete_history_proven"`
	FundingAllocated      bool                                `json:"funding_allocated"`
	PostingApplied        bool                                `json:"posting_applied"`
	SettlementAuthorized  bool                                `json:"settlement_authorized"`
	TradingAuthorized     bool                                `json:"trading_authorized"`
}

// root is a pre-existing, dedicated *offline* folder. Reusing different roots
// creates independent registries and cannot prevent cross-root allocation.
// The constructor creates no directories and never accesses Binance.
type ExternalGlobalEvidenceReservationStore struct{ dir, root string }

func NewExternalGlobalEvidenceReservationStore(root string) (*ExternalGlobalEvidenceReservationStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("explicit global reservation root required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe global reservation root")
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &ExternalGlobalEvidenceReservationStore{dir: filepath.Join(canonical, "external_global_evidence_reservations"), root: canonical}, nil
}

func (s *ExternalGlobalEvidenceReservationStore) checkDir(create bool) (bool, error) {
	if s == nil || s.dir == "" {
		return false, errors.New("invalid global reservation store")
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return false, err
		}
	}
	info, err := os.Lstat(s.dir)
	if errors.Is(err, os.ErrNotExist) && !create {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("unsafe global reservation directory")
	}
	return true, nil
}

func checkedFromClaims(claims []ExternalGlobalEvidenceClaim) []externalCrossEventChecked {
	out := make([]externalCrossEventChecked, 0, len(claims))
	for _, c := range claims {
		out = append(out, externalCrossEventChecked{
			member: ExternalCrossEventMember{ClientOrderID: c.ClientOrderID, ModelID: c.ModelID,
				PositionSide: c.PositionSide, EntryOrderID: c.EntryOrderID, ExitOrderID: c.ExitOrderID,
				FirstEntryMS: c.FirstEntryMS, LastExitMS: c.LastExitMS,
				BoundedFinancialSHA256: c.BoundedFinancialSHA256, OriginalEvidenceSHA256: c.OriginalEvidenceSHA256},
			entryTradeIDs: c.EntryTradeIDs, exitTradeIDs: c.ExitTradeIDs, fundingTranIDs: c.FundingTranIDs,
		})
	}
	return out
}

func validIDs(ids []int64) bool {
	last := int64(0)
	for _, id := range ids {
		if id <= last {
			return false
		}
		last = id
	}
	return true
}

func (r ExternalGlobalEvidenceReservation) validate(previous string, seq int) error {
	if r.Schema != 1 || r.Stage != ExternalGlobalEvidenceReserved || r.Sequence != seq ||
		r.PreviousChecksum != previous || !externalReviewHash.MatchString(r.BatchAuditSHA256) ||
		!externalReviewHash.MatchString(r.BatchKey) || r.BatchKey != r.key() || r.Checksum != r.digest() ||
		r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized || len(r.Claims) < 2 || len(r.Claims) > 64 {
		return errors.New("invalid, torn, or authorizing global reservation")
	}
	last := ""
	for _, c := range r.Claims {
		if !externalReviewID.MatchString(c.ClientOrderID) || c.ClientOrderID <= last || c.ModelID == "" ||
			(c.PositionSide != Long && c.PositionSide != Short) || c.EntryOrderID <= 0 || c.ExitOrderID <= 0 ||
			c.EntryOrderID == c.ExitOrderID || c.FirstEntryMS <= 0 || c.LastExitMS < c.FirstEntryMS ||
			len(c.EntryTradeIDs) == 0 || len(c.ExitTradeIDs) == 0 ||
			!validIDs(c.EntryTradeIDs) || !validIDs(c.ExitTradeIDs) || !validIDs(c.FundingTranIDs) ||
			!externalReviewHash.MatchString(c.BoundedFinancialSHA256) || !externalReviewHash.MatchString(c.OriginalEvidenceSHA256) {
			return errors.New("invalid global evidence claim")
		}
		last = c.ClientOrderID
	}
	if len(crossEventConflicts(checkedFromClaims(r.Claims))) != 0 {
		return errors.New("intra-batch evidence conflict")
	}
	return nil
}

func reservationSlot(seq int) string { return fmt.Sprintf("%08d.json", seq) }

// Inspect verifies every serialized sequence and all cross-batch collisions.
// A completely empty directory is allowed; a corrupt, orphan, partial, or
// symlinked slot fails closed, including after a process restart.
func (s *ExternalGlobalEvidenceReservationStore) Inspect(ctx context.Context) (ExternalGlobalEvidenceReservationState, error) {
	var zero ExternalGlobalEvidenceReservationState
	if ctx == nil {
		return zero, errors.New("context required")
	}
	exists, err := s.checkDir(false)
	if err != nil {
		return zero, err
	}
	out := ExternalGlobalEvidenceReservationState{Schema: 1, Stage: "GLOBAL_EVIDENCE_REGISTRY_READ_ONLY_NOT_POSTED", Reservations: []ExternalGlobalEvidenceReservation{}}
	if !exists {
		return out, nil
	}
	dir, err := os.ReadDir(s.dir)
	if err != nil {
		return zero, err
	}
	if len(dir) > 32 {
		return zero, errors.New("too many reservation slots")
	}
	prev := ""
	all := make([]ExternalGlobalEvidenceClaim, 0)
	for i, e := range dir {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		n := i + 1
		if e.Name() != reservationSlot(n) || !e.Type().IsRegular() || e.Type()&os.ModeSymlink != 0 {
			return zero, fmt.Errorf("orphan, invalid, or missing reservation slot %d", n)
		}
		// strictReviewJSON enforces bounds, unknown fields, and trailing JSON.
		var r ExternalGlobalEvidenceReservation
		if err := strictReviewJSON(filepath.Join(s.dir, e.Name()), &r); err != nil {
			return zero, fmt.Errorf("slot %d: %w", n, err)
		}
		if err := r.validate(prev, n); err != nil {
			return zero, fmt.Errorf("slot %d: %w", n, err)
		}
		all = append(all, r.Claims...)
		if len(all) > 64 {
			return zero, errors.New("global reservation capacity 64 exceeded")
		}
		prev = r.Checksum
		out.Reservations = append(out.Reservations, r)
	}
	seen := map[string]bool{}
	for _, c := range all {
		if seen[c.ClientOrderID] {
			return zero, errors.New("global reservation repeated client order identity")
		}
		seen[c.ClientOrderID] = true
	}
	if len(crossEventConflicts(checkedFromClaims(all))) != 0 {
		return zero, errors.New("global evidence collision detected")
	}
	out.TotalClaims = len(all)
	out.LastChecksum = prev
	return out, nil
}

func externalGlobalClaimFromMember(m ExternalCrossEventMember, archive PackageAIncidentArchive) ExternalGlobalEvidenceClaim {
	c := ExternalGlobalEvidenceClaim{
		ClientOrderID: m.ClientOrderID, ModelID: m.ModelID, PositionSide: m.PositionSide,
		EntryOrderID: m.EntryOrderID, ExitOrderID: m.ExitOrderID, FirstEntryMS: m.FirstEntryMS,
		LastExitMS: m.LastExitMS, BoundedFinancialSHA256: m.BoundedFinancialSHA256,
		OriginalEvidenceSHA256: m.OriginalEvidenceSHA256,
	}
	for _, t := range archive.Close.EntryFills {
		c.EntryTradeIDs = append(c.EntryTradeIDs, t.TradeID)
	}
	for _, t := range archive.Close.ExitFills {
		c.ExitTradeIDs = append(c.ExitTradeIDs, t.TradeID)
	}
	for _, f := range archive.Close.FundingRows {
		c.FundingTranIDs = append(c.FundingTranIDs, f.TranID)
	}
	sort.Slice(c.EntryTradeIDs, func(i, j int) bool { return c.EntryTradeIDs[i] < c.EntryTradeIDs[j] })
	sort.Slice(c.ExitTradeIDs, func(i, j int) bool { return c.ExitTradeIDs[i] < c.ExitTradeIDs[j] })
	sort.Slice(c.FundingTranIDs, func(i, j int) bool { return c.FundingTranIDs[i] < c.FundingTranIDs[j] })
	return c
}

// ReserveBatch checks original snapshots, creates one immutable NON-POSTING
// slot, and returns the observed chain. It deliberately does not accept any
// approval, financial amount, API key, or external transaction transport.
// On concurrent writes, the loser re-reads the entire chain and rechecks
// collisions. A torn writer is never assumed to have failed safely.
func (s *ExternalGlobalEvidenceReservationStore) ReserveBatch(ctx context.Context, events []ExternalFinancialEventSource) (ExternalGlobalEvidenceReservationState, error) {
	var zero ExternalGlobalEvidenceReservationState
	if s == nil || s.root == "" {
		return zero, errors.New("invalid global reservation store")
	}
	for _, e := range events {
		source, err := filepath.Abs(e.DataDir)
		if err != nil {
			return zero, err
		}
		source, err = filepath.EvalSymlinks(source)
		if err != nil {
			return zero, err
		}
		rel, err := filepath.Rel(source, s.root)
		if err != nil {
			return zero, err
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
			return zero, errors.New("global registry root cannot be inside original incident data")
		}
	}
	audit, err := InspectExternalCrossEventFinancials(ctx, events)
	if err != nil {
		return zero, err
	}
	if audit.Stage != ExternalCrossEventNoConflict || len(audit.Conflicts) != 0 || audit.PostingApplied || audit.SettlementAuthorized || audit.TradingAuthorized {
		return zero, errors.New("cross-event evidence conflicts or unauthorized report")
	}
	byID := make(map[string]PackageAIncidentArchive, len(events))
	for _, e := range events {
		byID[e.ClientOrderID] = e.Incident
	}
	r := ExternalGlobalEvidenceReservation{Schema: 1, Stage: ExternalGlobalEvidenceReserved,
		BatchAuditSHA256: audit.DigestSHA256, Claims: make([]ExternalGlobalEvidenceClaim, 0, len(audit.Members))}
	for _, m := range audit.Members {
		r.Claims = append(r.Claims, externalGlobalClaimFromMember(m, byID[m.ClientOrderID]))
	}
	r.BatchKey = r.key()
	r.Sequence = 1
	r.Checksum = r.digest()
	if err := r.validate("", 1); err != nil {
		return zero, err
	}
	for attempt := 0; attempt < 32; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		state, err := s.Inspect(ctx)
		if err != nil {
			return zero, err
		}
		for _, old := range state.Reservations {
			if old.BatchKey == r.BatchKey {
				// Revalidation of the signed-source-derived batch occurred above.
				return state, nil
			}
		}
		combined := make([]ExternalGlobalEvidenceClaim, 0, state.TotalClaims+len(r.Claims))
		for _, old := range state.Reservations {
			combined = append(combined, old.Claims...)
		}
		combined = append(combined, r.Claims...)
		if len(combined) > 64 {
			return zero, errors.New("global registration capacity exceeded")
		}
		ids := make(map[string]bool, len(combined))
		for _, c := range combined {
			if ids[c.ClientOrderID] {
				return zero, errors.New("global client order ID already reserved")
			}
			ids[c.ClientOrderID] = true
		}
		if len(crossEventConflicts(checkedFromClaims(combined))) != 0 {
			return zero, errors.New("global reservation evidence conflict")
		}
		r.Sequence = len(state.Reservations) + 1
		r.PreviousChecksum = state.LastChecksum
		r.Checksum = r.digest()
		if err := r.validate(r.PreviousChecksum, r.Sequence); err != nil {
			return zero, err
		}
		// The offline snapshots can change while another process observes
		// the registry; re-validate immediately before the only write.
		fresh, err := InspectExternalCrossEventFinancials(ctx, events)
		if err != nil {
			return zero, err
		}
		if fresh.DigestSHA256 != audit.DigestSHA256 {
			return zero, errors.New("original evidence drift before reservation")
		}
		if _, err := s.checkDir(true); err != nil {
			return zero, err
		}
		path := filepath.Join(s.dir, reservationSlot(r.Sequence))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return zero, err
		}
		raw, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			_ = f.Close()
			return zero, err
		}
		raw = append(raw, '\n')
		if _, err = f.Write(raw); err != nil {
			_ = f.Close()
			return zero, err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return zero, err
		}
		if err = f.Close(); err != nil {
			return zero, err
		}
		// On platforms that support directory fsync, persist the name too.
		// A dir fsync failure is conservatively treated as an error.
		if err = syncReplayDir(s.dir); err != nil {
			return zero, err
		}
		observed, err := s.Inspect(ctx)
		if err != nil {
			return zero, err
		}
		return observed, nil
	}
	return zero, errors.New("concurrent registry modification: retry only after operator review")
}
