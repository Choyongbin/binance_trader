package trading

// Dual-ledger transaction envelope: an immutable, non-posting rehearsal that
// binds the original execution lifecycle and ownership evidence to one common
// signed-close projection. Both *proposed* updates live in the same checksummed
// file; neither update is ever written to the real trading ledgers.
//
// This is NOT an atomic posting implementation. Real posting remains blocked
// until complete signed history, funding attribution and explicit settlement
// authorization exist, with independently tested roll-forward semantics.
import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	ExternalDualLedgerPrepared = "DUAL_LEDGER_PREPARED_NOT_POSTED"
	ExternalDualLedgerVerified = "DUAL_LEDGER_VERIFIED_NOT_POSTED"
)

type ExternalCloseDualLedgerEnvelope struct {
	Schema                 int            `json:"schema"`
	Stage                  string         `json:"stage"`
	ClientOrderID          string         `json:"client_order_id"`
	ModelID                string         `json:"model_id"`
	PositionSide           PositionSide   `json:"position_side"`
	TransactionKey         string         `json:"transaction_key"`
	ShadowReplayedSHA256   string         `json:"shadow_replayed_sha256"`
	LocalEvidenceSHA256    string         `json:"local_evidence_sha256"`
	ObservedLifecycleStage ExecutionStage `json:"observed_lifecycle_stage"`
	// Hypothetical changes; nothing here is a financial record or a command.
	LifecycleProposal       string `json:"lifecycle_proposal"`
	OwnershipProposal       string `json:"ownership_proposal"`
	ProposedExitQuantityBTC string `json:"proposed_exit_quantity_btc"`
	SimulatedRemainingBTC   string `json:"simulated_remaining_btc"`
	NetExcludingFundingUSDT string `json:"provisional_net_excluding_funding_usdt"`
	PreviousChecksum        string `json:"previous_checksum,omitempty"`
	Checksum                string `json:"checksum"`
	LifecycleChanged        bool   `json:"lifecycle_changed"`
	OwnershipChanged        bool   `json:"ownership_changed"`
	PostingApplied          bool   `json:"posting_applied"`
	SettlementAuthorized    bool   `json:"settlement_authorized"`
	TradingAuthorized       bool   `json:"trading_authorized"`
}

func (r ExternalCloseDualLedgerEnvelope) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

func validateDualLedgerEnvelope(r, expected ExternalCloseDualLedgerEnvelope) error {
	if r.Schema != 1 || r.Stage != expected.Stage || r.Checksum != r.digest() ||
		r.Checksum != expected.Checksum || !externalReviewID.MatchString(r.ClientOrderID) ||
		!externalReviewHash.MatchString(r.TransactionKey) ||
		!externalReviewHash.MatchString(r.LocalEvidenceSHA256) ||
		!externalReviewHash.MatchString(r.ShadowReplayedSHA256) ||
		r.LifecycleChanged || r.OwnershipChanged || r.PostingApplied ||
		r.SettlementAuthorized || r.TradingAuthorized {
		return errors.New("unsafe or inconsistent dual-ledger NON-POSTED envelope")
	}
	return nil
}

type ExternalCloseDualLedgerPlanStore struct {
	mu     sync.Mutex
	shadow *ExternalCloseShadowLedger
	dir    string
}

// Construction is read-only; the original trading records are never opened
// for mutation by this store.
func NewExternalCloseDualLedgerPlanStore(dataDir string) (*ExternalCloseDualLedgerPlanStore, error) {
	shadow, err := NewExternalCloseShadowLedger(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(shadow.intents.replay.proposal.dataDir, "trading", "external_close_dual_ledger_plan")
	if fi, e := os.Lstat(dir); e == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe dual-ledger plan directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	return &ExternalCloseDualLedgerPlanStore{shadow: shadow, dir: dir}, nil
}

func (s *ExternalCloseDualLedgerPlanStore) path(stage string) string {
	if stage == ExternalDualLedgerPrepared {
		return filepath.Join(s.dir, "prepared.json")
	}
	return filepath.Join(s.dir, "verified.json")
}

func (s *ExternalCloseDualLedgerPlanStore) checkDir(create bool) error {
	if s == nil || s.shadow == nil {
		return errors.New("nil dual-ledger store")
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	fi, err := os.Lstat(s.dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe dual-ledger plan directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if (ent.Name() != "prepared.json" && ent.Name() != "verified.json") ||
			ent.IsDir() || ent.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected dual-ledger plan entry: %s", ent.Name())
		}
	}
	return nil
}

func (s *ExternalCloseDualLedgerPlanStore) expected(id string) (ExternalCloseDualLedgerEnvelope, error) {
	if s == nil || !externalReviewID.MatchString(id) {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("invalid client order identity")
	}
	root, replayed, err := s.shadow.Load(id)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if replayed == nil {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("sealed shadow replay required")
	}
	proposal, err := s.shadow.intents.replay.proposal.Load(id)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	dataDir := s.shadow.intents.replay.proposal.dataDir
	audit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	var target *ExecutionLifecycleAuditItem
	for i := range audit.Items {
		if audit.Items[i].ClientOrderID == id {
			target = &audit.Items[i]
			break
		}
	}
	// Legacy local entry journals may predate the execution-lifecycle archive.
	// Represent that explicitly as a blocker in the NON-POSTED envelope. Never
	// synthesize a real lifecycle or infer that the order was settled.
	observedStage := ExecutionStage("MISSING_LIFECYCLE_BLOCKED")
	if target != nil {
		if target.Stage == ExecutionSettled || target.PositionSide != root.PositionSide {
			return ExternalCloseDualLedgerEnvelope{}, errors.New("settled or conflicting original lifecycle")
		}
		observedStage = target.Stage
	}
	if audit.State == ExecutionAuditBlocked || audit.TradingAuthorized {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("unsafe execution lifecycle audit")
	}
	local, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if local != proposal.LocalEvidenceSHA256 {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("original lifecycle or ownership evidence changed")
	}
	// A composite record is sealed atomically as a *preview* of both required
	// reconciliations. No lifecycle state or ownership quantity is mutated.
	record := ExternalCloseDualLedgerEnvelope{
		Schema: 1, Stage: ExternalDualLedgerPrepared, ClientOrderID: id,
		ModelID: root.ModelID, PositionSide: root.PositionSide,
		TransactionKey: root.TransactionKey, ShadowReplayedSHA256: replayed.Checksum,
		LocalEvidenceSHA256: local, ObservedLifecycleStage: observedStage,
		LifecycleProposal:       "RECOVER_SIGNED_ENTRY_BEFORE_EXTERNAL_CLOSE_POSTING",
		OwnershipProposal:       "VERIFY_SHARED_HEDGE_SIDE_BEFORE_APPLYING_EXIT",
		ProposedExitQuantityBTC: root.ExitQuantity,
		SimulatedRemainingBTC:   root.SimulatedRemainingQuantity,
		NetExcludingFundingUSDT: root.NetExcludingFunding,
	}
	record.Checksum = record.digest()
	return record, nil
}

func expectedDualLedgerVerified(root ExternalCloseDualLedgerEnvelope) ExternalCloseDualLedgerEnvelope {
	ev := root
	ev.Stage = ExternalDualLedgerVerified
	ev.PreviousChecksum = root.Checksum
	ev.Checksum = ev.digest()
	return ev
}

// Load rechecks all preceding signed/local evidence and both immutable stages.
// A truncated write or orphan event is never repaired automatically.
func (s *ExternalCloseDualLedgerPlanStore) Load(id string) (ExternalCloseDualLedgerEnvelope, *ExternalCloseDualLedgerEnvelope, error) {
	expected, err := s.expected(id)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	if err := s.checkDir(false); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	var root ExternalCloseDualLedgerEnvelope
	if err := strictReviewJSON(s.path(ExternalDualLedgerPrepared), &root); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	if err := validateDualLedgerEnvelope(root, expected); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	var verified ExternalCloseDualLedgerEnvelope
	err = strictReviewJSON(s.path(ExternalDualLedgerVerified), &verified)
	if errors.Is(err, os.ErrNotExist) {
		return root, nil, nil
	}
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	if err := validateDualLedgerEnvelope(verified, expectedDualLedgerVerified(root)); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, nil, err
	}
	return root, &verified, nil
}

func (s *ExternalCloseDualLedgerPlanStore) Prepare(id string) (ExternalCloseDualLedgerEnvelope, error) {
	if s == nil {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("nil store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := s.expected(id)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if err := s.checkDir(true); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if len(entries) > 0 {
		got, _, err := s.Load(id)
		return got, err
	}
	if _, err := s.expected(id); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if err := exclusiveReviewWrite(s.path(ExternalDualLedgerPrepared), expected); err != nil {
		if got, _, e := s.Load(id); e == nil && got.Checksum == expected.Checksum {
			return got, nil
		}
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	got, _, err := s.Load(id)
	return got, err
}

// Verify permanently appends the re-derived two-ledger preview in one JSON
// envelope. It does not commit changes to either actual ledger.
func (s *ExternalCloseDualLedgerPlanStore) Verify(id string) (ExternalCloseDualLedgerEnvelope, error) {
	if s == nil {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("nil store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, already, err := s.Load(id)
	if err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if already != nil {
		return *already, nil
	}
	expected := expectedDualLedgerVerified(root)
	if _, err := s.expected(id); err != nil {
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	if err := exclusiveReviewWrite(s.path(ExternalDualLedgerVerified), expected); err != nil {
		_, got, e := s.Load(id)
		if e == nil && got != nil && got.Checksum == expected.Checksum {
			return *got, nil
		}
		return ExternalCloseDualLedgerEnvelope{}, err
	}
	_, got, err := s.Load(id)
	if err != nil || got == nil {
		return ExternalCloseDualLedgerEnvelope{}, errors.New("dual-ledger verification missing or invalid")
	}
	return *got, nil
}
