package trading

// dev.3.128: OFFLINE-only crash-safe page capture rehearsal.
// This is not a Binance transport, account attestation, settlement permission,
// or a source of trusted signed history. The caller supplies a simulated reader.
// Each completed sub-window is an immutable, fsynced, chained checkpoint.
// A partial, foreign, missing or altered slot is never repaired automatically.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

const (
	ExternalHistoryCapturePartial  = "OFFLINE_CAPTURE_INCOMPLETE_NOT_ATTESTED"
	ExternalHistoryCaptureComplete = "OFFLINE_CAPTURE_MATCHED_BOUNDED_NOT_ATTESTED"
	externalCaptureMaxSlots        = 64
	externalCaptureWindowMS        = int64(24 * time.Hour / time.Millisecond)
)

// A successful call must represent BOTH responses to exactly [from,to]. A
// nil slice means the query was not captured; an empty non-nil slice means a
// captured zero-row response. Transports, timestamps and account authenticity
// are deliberately outside this simulation contract.
type ExternalHistoryCaptureReader func(ctx context.Context, from, to int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error)

type ExternalHistoryCaptureHeader struct {
	Schema         int    `json:"schema"`
	ClientOrderID  string `json:"client_order_id"`
	IncidentSHA256 string `json:"incident_archive_sha256"`
	SourceSHA256   string `json:"offline_source_sha256"`
	FromMS         int64  `json:"from_ms"`
	ToMS           int64  `json:"to_ms"`
	Scope          string `json:"scope"`
	Checksum       string `json:"checksum"`
}

func (h ExternalHistoryCaptureHeader) digest() string { h.Checksum = ""; return externalCloseHash(h) }

type ExternalHistoryCaptureCheckpoint struct {
	Schema           int                         `json:"schema"`
	Sequence         int                         `json:"sequence"`
	HeaderChecksum   string                      `json:"header_checksum"`
	PreviousChecksum string                      `json:"previous_checksum"`
	Window           ExternalSignedHistoryWindow `json:"window"`
	Checksum         string                      `json:"checksum"`
}

func (c ExternalHistoryCaptureCheckpoint) digest() string {
	c.Checksum = ""
	return externalCloseHash(c)
}

type ExternalHistoryCaptureSeal struct {
	Schema                 int    `json:"schema"`
	HeaderChecksum         string `json:"header_checksum"`
	LastCheckpointChecksum string `json:"last_checkpoint_checksum"`
	WindowCount            int    `json:"window_count"`
	ContinuitySHA256       string `json:"continuity_sha256"`
	Checksum               string `json:"checksum"`
}

func (v ExternalHistoryCaptureSeal) digest() string { v.Checksum = ""; return externalCloseHash(v) }

type ExternalHistoryCaptureInspection struct {
	Schema                    int                              `json:"schema"`
	Stage                     string                           `json:"stage"`
	ClientOrderID             string                           `json:"client_order_id"`
	CompletedWindows          int                              `json:"completed_windows"`
	NextFromMS                int64                            `json:"next_from_ms"`
	ToMS                      int64                            `json:"to_ms"`
	LastChecksum              string                           `json:"last_checksum,omitempty"`
	Complete                  bool                             `json:"complete"`
	BoundedContinuity         *ExternalSignedHistoryContinuity `json:"bounded_continuity,omitempty"`
	WholeAccountHistoryProven bool                             `json:"whole_account_history_proven"`
	ExchangeSourceAttested    bool                             `json:"exchange_source_attested"`
	PostingApplied            bool                             `json:"posting_applied"`
	SettlementAuthorized      bool                             `json:"settlement_authorized"`
	TradingAuthorized         bool                             `json:"trading_authorized"`
}

type ExternalHistoryCaptureStore struct{ dir string }

// root must exist and be an independent offline directory. This constructor
// does not create or modify any file; no Binance client is accessible.
func NewExternalHistoryCaptureStore(root string) (*ExternalHistoryCaptureStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("explicit offline capture root required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fi, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe offline capture root")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &ExternalHistoryCaptureStore{dir: filepath.Join(resolved, "external_signed_history_capture")}, nil
}
func (s *ExternalHistoryCaptureStore) checkDir(create bool) (bool, error) {
	if s == nil || s.dir == "" {
		return false, errors.New("nil capture store")
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return false, err
		}
	}
	fi, err := os.Lstat(s.dir)
	if errors.Is(err, os.ErrNotExist) && !create {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("unsafe capture checkpoint directory")
	}
	return true, nil
}
func captureSlot(i int) string { return fmt.Sprintf("%08d.json", i) }
func captureHeader(clientID string, incident PackageAIncidentArchive, sourceHash string) (ExternalHistoryCaptureHeader, error) {
	h := ExternalHistoryCaptureHeader{Schema: 1, ClientOrderID: clientID, IncidentSHA256: incident.Checksum, SourceSHA256: sourceHash,
		FromMS: incident.Close.HistoryFromMS, ToMS: incident.Close.HistoryToMS, Scope: ExternalSignedHistoryScope}
	if !externalReviewID.MatchString(clientID) || incident.ClientOrderID != clientID ||
		!externalReviewHash.MatchString(incident.Checksum) || !externalReviewHash.MatchString(sourceHash) ||
		h.FromMS <= 0 || h.ToMS < h.FromMS || h.ToMS-h.FromMS > signedCloseMaxSpan.Milliseconds() {
		return h, errors.New("invalid offline capture identity or bounded interval")
	}
	h.Checksum = h.digest()
	return h, nil
}
func safeSeparateCaptureRoot(dataDir, captureDir string) error {
	data, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return err
	}
	parent := filepath.Dir(captureDir)
	rel, err := filepath.Rel(data, parent)
	if err != nil {
		return err
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
		return errors.New("capture checkpoint root must be outside original trading data")
	}
	return nil
}
func validCaptureWindow(w ExternalSignedHistoryWindow) error {
	if w.FromMS <= 0 || w.ToMS < w.FromMS || w.ToMS-w.FromMS >= externalCaptureWindowMS ||
		w.Trades == nil || w.Funding == nil || w.TradePageLimit != externalHistoryPageLimit || w.FundingPageLimit != externalHistoryPageLimit ||
		len(w.Trades) >= externalHistoryPageLimit || len(w.Funding) >= externalHistoryPageLimit ||
		w.TradeRowsSHA256 != externalCloseHash(w.Trades) || w.FundingRowsSHA256 != externalCloseHash(w.Funding) {
		return errors.New("missing, truncated or corrupt captured signed GET responses")
	}
	for _, v := range w.Trades {
		if v.Symbol != "BTCUSDT" || v.TradeID <= 0 || v.OrderID <= 0 || v.TimeMS < w.FromMS || v.TimeMS > w.ToMS ||
			(v.Side != "BUY" && v.Side != "SELL") || (v.PositionSide != "LONG" && v.PositionSide != "SHORT") || v.CommissionAsset != "USDT" {
			return errors.New("invalid trade row in captured page")
		}
		if _, err := positiveRat(v.Price); err != nil {
			return err
		}
		if _, err := positiveRat(v.Quantity); err != nil {
			return err
		}
		fee, err := signedRat(v.Commission)
		if err != nil {
			return err
		}
		if fee.Sign() < 0 {
			return errors.New("negative commission")
		}
		if _, err := signedRat(v.RealizedPnL); err != nil {
			return err
		}
	}
	for _, v := range w.Funding {
		if v.Symbol != "BTCUSDT" || v.IncomeType != "FUNDING_FEE" || v.Asset != "USDT" || v.TranID <= 0 ||
			v.TimeMS < w.FromMS || v.TimeMS > w.ToMS {
			return errors.New("invalid funding row in captured page")
		}
		if _, err := signedRat(v.Income); err != nil {
			return err
		}
	}
	return nil
}

// scan validates the complete, immutable prefix. Incomplete slots, file gaps,
// strange filenames, symlinks, changed source identities and hash-chain drift
// are terminal blockers until independently reviewed out of band.
func (s *ExternalHistoryCaptureStore) scan(ctx context.Context, want ExternalHistoryCaptureHeader) ([]ExternalSignedHistoryWindow, string, int64, bool, error) {
	if ctx == nil {
		return nil, "", 0, false, errors.New("context required")
	}
	exists, err := s.checkDir(false)
	if err != nil {
		return nil, "", 0, false, err
	}
	if !exists {
		return nil, "", want.FromMS, false, nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, "", 0, false, err
	}
	if len(entries) == 0 {
		return nil, "", 0, false, errors.New("capture directory exists without immutable header")
	}
	var got ExternalHistoryCaptureHeader
	if err := strictReviewJSON(filepath.Join(s.dir, "header.json"), &got); err != nil {
		return nil, "", 0, false, err
	}
	if got != want || got.Checksum != got.digest() {
		return nil, "", 0, false, errors.New("capture header/source/checksum mismatch")
	}
	if len(entries) > externalCaptureMaxSlots+2 {
		return nil, "", 0, false, errors.New("capture checkpoint capacity exceeded")
	}
	// ReadDir sorts by name. The header must be present and every other
	// entry must be the next sequential checkpoint with no gaps.
	if entries[0].Name() != "00000001.json" && entries[0].Name() != "header.json" {
		return nil, "", 0, false, errors.New("foreign checkpoint file")
	}
	windows := make([]ExternalSignedHistoryWindow, 0, len(entries)-1)
	next, prev := want.FromMS, ""
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, "", 0, false, err
		}
		if e.Name() == "seal.json" {
			if !e.Type().IsRegular() {
				return nil, "", 0, false, errors.New("unsafe final capture seal type")
			}
			continue
		}
		if e.Name() == "header.json" {
			if !e.Type().IsRegular() {
				return nil, "", 0, false, errors.New("unsafe header type")
			}
			continue
		}
		if e.Name() != captureSlot(len(windows)+1) || !e.Type().IsRegular() {
			return nil, "", 0, false, errors.New("missing/foreign/unsafe capture checkpoint slot")
		}
		var c ExternalHistoryCaptureCheckpoint
		if err := strictReviewJSON(filepath.Join(s.dir, e.Name()), &c); err != nil {
			return nil, "", 0, false, err
		}
		if c.Schema != 1 || c.Sequence != len(windows)+1 || c.HeaderChecksum != want.Checksum || c.PreviousChecksum != prev ||
			c.Checksum != c.digest() || c.Window.FromMS != next || c.Window.ToMS > want.ToMS {
			return nil, "", 0, false, errors.New("capture sequence/hash/bounds mismatch")
		}
		if err := validCaptureWindow(c.Window); err != nil {
			return nil, "", 0, false, err
		}
		windows = append(windows, c.Window)
		prev = c.Checksum
		next = c.Window.ToMS + 1
	}
	// An existing completion seal makes a lost/truncated slot a fatal error,
	// never a fresh gap to be re-fetched or silently reclassified as partial.
	if _, err := os.Lstat(filepath.Join(s.dir, "seal.json")); err == nil {
		var seal ExternalHistoryCaptureSeal
		if err := strictReviewJSON(filepath.Join(s.dir, "seal.json"), &seal); err != nil {
			return nil, "", 0, false, err
		}
		if seal.Schema != 1 || seal.HeaderChecksum != want.Checksum || seal.LastCheckpointChecksum != prev ||
			seal.WindowCount != len(windows) || next <= want.ToMS || !externalReviewHash.MatchString(seal.ContinuitySHA256) || seal.Checksum != seal.digest() {
			return nil, "", 0, false, errors.New("broken completion seal, missing checkpoint, or incomplete capture")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, "", 0, false, err
	}
	return windows, prev, next, true, nil
}
func (s *ExternalHistoryCaptureStore) Inspect(ctx context.Context, dataDir, clientID string, incident PackageAIncidentArchive, sourceHash string) (ExternalHistoryCaptureInspection, error) {
	var zero ExternalHistoryCaptureInspection
	want, err := captureHeader(clientID, incident, sourceHash)
	if err != nil {
		return zero, err
	}
	if err := safeSeparateCaptureRoot(dataDir, s.dir); err != nil {
		return zero, err
	}
	windows, last, next, exists, err := s.scan(ctx, want)
	if err != nil {
		return zero, err
	}
	state := ExternalHistoryCaptureInspection{Schema: 1, Stage: ExternalHistoryCapturePartial, ClientOrderID: clientID,
		CompletedWindows: len(windows), NextFromMS: next, ToMS: want.ToMS, LastChecksum: last}
	if !exists {
		return state, nil
	}
	if next <= want.ToMS {
		return state, nil
	}
	// No seal is an incomplete finalization, never a positive completion.
	sealPath := filepath.Join(s.dir, "seal.json")
	if _, err := os.Lstat(sealPath); errors.Is(err, os.ErrNotExist) {
		return state, nil
	} else if err != nil {
		return zero, err
	}
	m := ExternalSignedHistoryManifest{Schema: 1, ClientOrderID: clientID, IncidentArchiveSHA256: incident.Checksum, Scope: ExternalSignedHistoryScope, Windows: windows}
	m.Checksum = m.digest()
	r, err := InspectExternalSignedHistoryContinuity(ctx, dataDir, clientID, incident, m)
	if err != nil {
		return zero, fmt.Errorf("final original signed history comparison blocked: %w", err)
	}
	// A second file scan catches source drift between manifest construction and
	// final validation; a simultaneous append is never treated as success.
	again, againLast, againNext, _, err := s.scan(ctx, want)
	if err != nil || againLast != last || againNext != next || len(again) != len(windows) {
		return zero, errors.New("capture checkpoint changed while finalizing")
	}
	var seal ExternalHistoryCaptureSeal
	if err := strictReviewJSON(sealPath, &seal); err != nil {
		return zero, err
	}
	if seal.ContinuitySHA256 != r.DigestSHA256 {
		return zero, errors.New("sealed continuity digest disagrees with current original evidence")
	}
	state.Stage = ExternalHistoryCaptureComplete
	state.Complete = true
	state.BoundedContinuity = &r
	return state, nil
}

// Capture only writes an independent offline store. Interrupted responses do
// not create completed pages. On restart it resumes at the first uncovered ms.
// Any response at limit splits the requested interval. A saturated single
// millisecond cannot be proven complete and is an unrecoverable blocker.
func (s *ExternalHistoryCaptureStore) Capture(ctx context.Context, dataDir, clientID string, incident PackageAIncidentArchive, sourceHash string, reader ExternalHistoryCaptureReader) (ExternalHistoryCaptureInspection, error) {
	var zero ExternalHistoryCaptureInspection
	if reader == nil || ctx == nil {
		return zero, errors.New("explicit simulated capture reader and context required")
	}
	want, err := captureHeader(clientID, incident, sourceHash)
	if err != nil {
		return zero, err
	}
	if err := safeSeparateCaptureRoot(dataDir, s.dir); err != nil {
		return zero, err
	}
	_, _, next, exists, err := s.scan(ctx, want)
	if err != nil {
		return zero, err
	}
	if !exists {
		if _, err := s.checkDir(true); err != nil {
			return zero, err
		}
		if err := exclusiveReviewWrite(filepath.Join(s.dir, "header.json"), want); err != nil {
			return zero, err
		}
	}
	// A successful page always has <1000 rows in EACH stream. The reader may
	// be called several times for a high-density range; none of the saturated
	// responses is a completed checkpoint.
	for next <= want.ToMS {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		windows, prev, observed, _, err := s.scan(ctx, want)
		if err != nil {
			return zero, err
		}
		if observed != next {
			return zero, errors.New("concurrent checkpoint write or progress drift")
		}
		if len(windows) >= externalCaptureMaxSlots {
			return zero, errors.New("capture window capacity exhausted")
		}
		end := next + externalCaptureWindowMS - 1
		if end > want.ToMS {
			end = want.ToMS
		}
		var t []exchange.AccountTrade
		var f []exchange.FundingIncome
		for {
			t, f, err = reader(ctx, next, end)
			if err != nil {
				return zero, err
			}
			if t == nil || f == nil {
				return zero, errors.New("missing one or both signed GET page responses")
			}
			if len(t) < externalHistoryPageLimit && len(f) < externalHistoryPageLimit {
				break
			}
			if end == next {
				return zero, errors.New("unresolvable page limit at one millisecond; cannot assert completeness")
			}
			end = next + (end-next)/2
		}
		w := ExternalSignedHistoryWindow{FromMS: next, ToMS: end, Trades: t, Funding: f, TradePageLimit: externalHistoryPageLimit, FundingPageLimit: externalHistoryPageLimit,
			TradeRowsSHA256: externalCloseHash(t), FundingRowsSHA256: externalCloseHash(f)}
		if err := validCaptureWindow(w); err != nil {
			return zero, err
		}
		c := ExternalHistoryCaptureCheckpoint{Schema: 1, Sequence: len(windows) + 1, HeaderChecksum: want.Checksum, PreviousChecksum: prev, Window: w}
		c.Checksum = c.digest()
		// O_EXCL collisions, partial writes, or fsync errors are BLOCKED, not
		// silently converted into another writer's success.
		if err := exclusiveReviewWrite(filepath.Join(s.dir, captureSlot(c.Sequence)), c); err != nil {
			return zero, err
		}
		next = end + 1
	}
	if _, err := os.Lstat(filepath.Join(s.dir, "seal.json")); errors.Is(err, os.ErrNotExist) {
		windows, prev, observed, _, err := s.scan(ctx, want)
		if err != nil {
			return zero, err
		}
		if observed <= want.ToMS {
			return zero, errors.New("cannot seal incomplete capture")
		}
		manifest := ExternalSignedHistoryManifest{Schema: 1, ClientOrderID: clientID, IncidentArchiveSHA256: incident.Checksum, Scope: ExternalSignedHistoryScope, Windows: windows}
		manifest.Checksum = manifest.digest()
		continuity, err := InspectExternalSignedHistoryContinuity(ctx, dataDir, clientID, incident, manifest)
		if err != nil {
			return zero, err
		}
		seal := ExternalHistoryCaptureSeal{Schema: 1, HeaderChecksum: want.Checksum, LastCheckpointChecksum: prev, WindowCount: len(windows), ContinuitySHA256: continuity.DigestSHA256}
		seal.Checksum = seal.digest()
		if err := exclusiveReviewWrite(filepath.Join(s.dir, "seal.json"), seal); err != nil {
			return zero, err
		}
	} else if err != nil {
		return zero, err
	}
	return s.Inspect(ctx, dataDir, clientID, incident, sourceHash)
}
