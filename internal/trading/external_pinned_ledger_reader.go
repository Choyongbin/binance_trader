package trading

// Common OFFLINE read boundary for two intentionally distinct data sources.
// REAL means existing execution files, not an exchange-confirmed financial
// ledger. SHADOW means a published *proposal*, not a committed financial
// transaction. No method on this reader writes or authorizes trading.
//
// Stop all writers and inspect a copied data directory: hashes and repeated
// reads detect many inconsistencies but are not an atomic filesystem snapshot.
import (
	"context"
	"errors"
	"fmt"
)

type ExternalLedgerSource string

const (
	ExternalLedgerSourceReal   ExternalLedgerSource = "REAL_EXECUTION_EVIDENCE_READ_ONLY"
	ExternalLedgerSourceShadow ExternalLedgerSource = "SHADOW_PROPOSAL_NOT_POSTED"
)

// The two source-specific pins are not interchangeable. A SHADOW read also
// requires the publication marker checksum, not merely the underlying SEAL.
type ExternalLedgerReadPin struct {
	RealEvidenceSHA256      string `json:"real_evidence_sha256,omitempty"`
	ShadowSealSHA256        string `json:"shadow_seal_sha256,omitempty"`
	ShadowPublicationSHA256 string `json:"shadow_publication_sha256,omitempty"`
}

// Exactly one of Real and Shadow must be present. Consumers must branch on
// Source; no common "committed" boolean is exposed by this interface.
type ExternalLedgerReadSnapshot struct {
	Schema               int                       `json:"schema"`
	Source               ExternalLedgerSource      `json:"source"`
	ClientOrderID        string                    `json:"client_order_id"`
	SourceSHA256         string                    `json:"source_sha256"`
	PublicationSHA256    string                    `json:"publication_sha256,omitempty"`
	Real                 *ExternalRealPinnedBundle `json:"real_execution,omitempty"`
	Shadow               *ExternalAtomicLedgerPair `json:"shadow_proposal_not_posted,omitempty"`
	ManualReviewRequired bool                      `json:"manual_review_required"`
	PostingApplied       bool                      `json:"posting_applied"`
	SettlementAuthorized bool                      `json:"settlement_authorized"`
	TradingAuthorized    bool                      `json:"trading_authorized"`
}

// ExternalLedgerReadPort is deliberately read-only. Source-specific ports
// reject pins belonging to another source before reading any files.
type ExternalLedgerReadPort interface {
	ReadPinned(ctx context.Context, pin ExternalLedgerReadPin) (ExternalLedgerReadSnapshot, error)
}

type ExternalRealReadPort struct{ dataDir, clientOrderID string }

func NewExternalRealReadPort(dataDir, clientOrderID string) (*ExternalRealReadPort, error) {
	if dataDir == "" || !externalReviewID.MatchString(clientOrderID) {
		return nil, errors.New("valid data path and client ID required")
	}
	return &ExternalRealReadPort{dataDir: dataDir, clientOrderID: clientOrderID}, nil
}

func (r *ExternalRealReadPort) ReadPinned(ctx context.Context, pin ExternalLedgerReadPin) (ExternalLedgerReadSnapshot, error) {
	var empty ExternalLedgerReadSnapshot
	if r == nil || ctx == nil || !externalReviewHash.MatchString(pin.RealEvidenceSHA256) ||
		pin.ShadowSealSHA256 != "" || pin.ShadowPublicationSHA256 != "" {
		return empty, errors.New("real reader requires only a real evidence SHA-256")
	}
	b, err := ReadPinnedRealLedgerBundle(ctx, r.dataDir, r.clientOrderID, pin.RealEvidenceSHA256)
	if err != nil {
		return empty, err
	}
	if b.ClientOrderID != r.clientOrderID || b.EvidenceSHA256 != pin.RealEvidenceSHA256 ||
		b.Stage != ExternalRealPinnedReadOnly || !b.ManualReviewRequired ||
		b.PostingApplied || b.SettlementAuthorized || b.TradingAuthorized {
		return empty, errors.New("invalid or authorizing real ledger projection")
	}
	return ExternalLedgerReadSnapshot{
		Schema: 1, Source: ExternalLedgerSourceReal, ClientOrderID: r.clientOrderID,
		SourceSHA256: b.EvidenceSHA256, Real: &b, ManualReviewRequired: true,
	}, nil
}

type ExternalShadowReadPort struct {
	store   *ExternalShadowCommitStore
	archive PackageAEntryArchive
}

func NewExternalShadowReadPort(store *ExternalShadowCommitStore, archive PackageAEntryArchive) (*ExternalShadowReadPort, error) {
	if store == nil || !externalReviewID.MatchString(store.id) {
		return nil, errors.New("valid event-scoped shadow store required")
	}
	return &ExternalShadowReadPort{store: store, archive: archive}, nil
}

func (r *ExternalShadowReadPort) ReadPinned(ctx context.Context, pin ExternalLedgerReadPin) (ExternalLedgerReadSnapshot, error) {
	var empty ExternalLedgerReadSnapshot
	if r == nil || r.store == nil || ctx == nil || pin.RealEvidenceSHA256 != "" ||
		!externalReviewHash.MatchString(pin.ShadowSealSHA256) ||
		!externalReviewHash.MatchString(pin.ShadowPublicationSHA256) {
		return empty, errors.New("shadow reader requires only sealed and published SHA-256 pins")
	}
	before, err := r.store.Inspect(ctx, r.archive, pin.ShadowSealSHA256)
	if err != nil {
		return empty, err
	}
	if !before.ShadowPublicationVisible || before.Stage != ExternalShadowCommitPublished ||
		before.ClientOrderID != r.store.id || before.PinnedSealSHA256 != pin.ShadowSealSHA256 ||
		before.ShadowPublishSHA256 != pin.ShadowPublicationSHA256 || before.PostingApplied ||
		before.SettlementAuthorized || before.TradingAuthorized {
		return empty, errors.New("shadow source is unpublished, stale, or claims posting authority")
	}
	pair, err := r.store.ReadShadowPair(ctx, r.archive, pin.ShadowSealSHA256)
	if err != nil {
		return empty, err
	}
	if pair.Stage != ExternalAtomicPairViewNotPosted || pair.ClientOrderID != r.store.id ||
		pair.CommitSHA256 != pin.ShadowSealSHA256 ||
		pair.Lifecycle.CommitSHA256 != pin.ShadowSealSHA256 ||
		pair.Ownership.CommitSHA256 != pin.ShadowSealSHA256 ||
		pair.Lifecycle.Applied || pair.Ownership.Applied || pair.PostingApplied ||
		pair.SettlementAuthorized || pair.TradingAuthorized {
		return empty, errors.New("shadow pair is inconsistent or implies financial posting")
	}
	after, err := r.store.Inspect(ctx, r.archive, pin.ShadowSealSHA256)
	if err != nil || before != after {
		return empty, errors.New("shadow publication changed during pinned read")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return ExternalLedgerReadSnapshot{
		Schema: 1, Source: ExternalLedgerSourceShadow, ClientOrderID: r.store.id,
		SourceSHA256: pin.ShadowSealSHA256, PublicationSHA256: before.ShadowPublishSHA256,
		Shadow: &pair, ManualReviewRequired: true,
	}, nil
}

// ReadExternalLedgerSnapshot chooses a source explicitly. There is no
// fallback from stale REAL to SHADOW (or vice versa), and a missing/foreign
// archive cannot be silently substituted. Only the SHADOW branch uses a
// Package A entry archive.
func ReadExternalLedgerSnapshot(ctx context.Context, dataDir, clientOrderID string,
	source ExternalLedgerSource, pin ExternalLedgerReadPin, archive *PackageAEntryArchive,
) (ExternalLedgerReadSnapshot, error) {
	switch source {
	case ExternalLedgerSourceReal:
		if archive != nil {
			return ExternalLedgerReadSnapshot{}, errors.New("real read must not accept a shadow archive")
		}
		r, err := NewExternalRealReadPort(dataDir, clientOrderID)
		if err != nil {
			return ExternalLedgerReadSnapshot{}, err
		}
		return r.ReadPinned(ctx, pin)
	case ExternalLedgerSourceShadow:
		if archive == nil {
			return ExternalLedgerReadSnapshot{}, errors.New("shadow archive required")
		}
		store, err := NewExternalShadowCommitStore(dataDir, clientOrderID)
		if err != nil {
			return ExternalLedgerReadSnapshot{}, fmt.Errorf("shadow read: %w", err)
		}
		r, err := NewExternalShadowReadPort(store, *archive)
		if err != nil {
			return ExternalLedgerReadSnapshot{}, err
		}
		return r.ReadPinned(ctx, pin)
	default:
		return ExternalLedgerReadSnapshot{}, errors.New("unsupported ledger source; explicit REAL or SHADOW required")
	}
}
