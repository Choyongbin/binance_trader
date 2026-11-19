package trading

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
	walletpkg "binance_trader/internal/wallet"
)

// Coordinator is a fail-closed PREVIEW library. An exclusive atomic directory
// gives ONE process the right to plan reservations in a shared data directory.
// After a crash the directory intentionally remains LOCKED; only a human may
// inspect the exchange and local files before releasing a stale lock. There is
// no automatic stale-PID unlocking, no HTTP route and no live-order wiring.
// Filesystem must provide atomic mkdir and O_EXCL (local NTFS is expected).
var ErrCoordinatorLocked = errors.New("거래 조정 잠금이 존재합니다. 실행 중인 다른 서버 또는 비정상 종료 여부를 확인하세요")

// ErrPooledSideExitInFlight means one durable EXIT reservation already owns
// the pooled Hedge side. Binance merges all owners on LONG/SHORT, so no other
// exit or re-entry on that side may be staged until the reservation is settled
// after a fresh signed order/fill/whole-position audit.
var ErrPooledSideExitInFlight = errors.New("동일 Hedge 방향의 청산 예약이 아직 독립 서명 주문·체결·전체 포지션 대조로 종료되지 않았습니다")

// ErrPooledSideMutationPending prevents starting an EXIT while an older entry
// reservation on that same pooled side remains unresolved. Otherwise that
// entry could fill while the exit is being reconciled and invalidate quantity
// ownership assumptions.
var ErrPooledSideMutationPending = errors.New("동일 Hedge 방향의 미종료 진입 예약이 있어 직렬 청산을 시작할 수 없습니다")
var reservationName = regexp.MustCompile(`^bt_[0-9a-f]{28}\.json$`)

type Coordinator struct {
	mu                               sync.Mutex
	root, lockDir, reserveDir, nonce string
	closed                           bool
}

type Reservation struct {
	Schema           int    `json:"schema"`
	ClientOrderID    string `json:"client_order_id"`
	Intent           Intent `json:"intent"`
	IntentDigest     string `json:"intent_digest"`
	ReservedQuantity string `json:"reserved_quantity"`
	// Model entry reserves collateral; an exit reserves only owner quantity.
	MarginUSDT string `json:"margin_usdt"`
	CreatedAt  string `json:"created_at"`
	Checksum   string `json:"checksum"`
}

func (r Reservation) digest() string {
	r.Checksum = ""
	b, _ := json.Marshal(r)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (r *Reservation) seal() { r.Checksum = r.digest() }

// reservationDoneMarker v2 makes the post-order ownership reconciliation
// durable. Schema 1 is retained read-only for cumulative compatibility; all
// new settlements write schema 2 with a checksum and signed snapshot facts.
type reservationDoneMarker struct {
	Schema              int          `json:"schema"`
	ClientOrderID       string       `json:"client_order_id"`
	ReservationChecksum string       `json:"reservation_checksum"`
	SettledAt           string       `json:"settled_at"`
	Action              Action       `json:"action,omitempty"`
	PositionSide        PositionSide `json:"position_side,omitempty"`
	SignedSnapshotAsOf  string       `json:"signed_snapshot_as_of,omitempty"`
	OwnerRemainingQty   string       `json:"owner_remaining_qty,omitempty"`
	ExchangeSideQty     string       `json:"exchange_side_qty,omitempty"`
	OwnershipReady      bool         `json:"ownership_ready,omitempty"`
	Checksum            string       `json:"checksum,omitempty"`
}

func (m reservationDoneMarker) digest() string {
	m.Checksum = ""
	b, _ := json.Marshal(m)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func decodeReservationDone(path string, r Reservation) (reservationDoneMarker, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return reservationDoneMarker{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var m reservationDoneMarker
	if err := dec.Decode(&m); err != nil {
		return m, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return m, errors.New("예약 종료 파일에 뒤따르는 데이터가 있습니다")
	}
	if m.ClientOrderID != r.ClientOrderID || m.ReservationChecksum != r.Checksum {
		return m, errors.New("예약 종료 파일 체크섬 불일치")
	}
	settled, err := time.Parse(time.RFC3339Nano, m.SettledAt)
	if err != nil || settled.After(time.Now().UTC().Add(time.Second)) {
		return m, errors.New("예약 종료 시각이 손상되었습니다")
	}
	if m.Schema == 1 {
		// Historical releases did not seal the done marker itself. Keep them
		// readable, but never manufacture v2 attestation fields for them.
		if m.Action != "" || m.PositionSide != "" || m.SignedSnapshotAsOf != "" || m.OwnerRemainingQty != "" || m.ExchangeSideQty != "" || m.OwnershipReady || m.Checksum != "" {
			return m, errors.New("구버전 예약 종료 파일에 예상하지 못한 필드가 있습니다")
		}
		return m, nil
	}
	if m.Schema != 2 || m.Action != r.Intent.Action || m.PositionSide != r.Intent.PositionSide || !m.OwnershipReady || m.Checksum == "" || m.Checksum != m.digest() {
		return m, errors.New("예약 종료 감사 증적이 손상되었습니다")
	}
	asOf, err := time.Parse(time.RFC3339Nano, m.SignedSnapshotAsOf)
	if err != nil || asOf.After(settled.Add(time.Second)) {
		return m, errors.New("예약 종료 서명 스냅샷 시각이 손상되었습니다")
	}
	owner, e1 := signedRat(m.OwnerRemainingQty)
	side, e2 := signedRat(m.ExchangeSideQty)
	if e1 != nil || e2 != nil || owner.Sign() < 0 || side.Sign() < 0 {
		return m, errors.New("예약 종료 후 수량 증적이 손상되었습니다")
	}
	return m, nil
}

type ReservationBasis struct {
	// This MUST be computed from independent authenticated exchange reads and
	// full imported fill history; caller-provided UI data is NOT trustworthy.
	Snapshot exchange.Snapshot
	Audit    OwnershipReport
	Quote    Quote
	// Assigned from validated, immutable Demo wallet ledger with independent
	// account-wide reconciliation. Empty means order planning is disabled.
	AutoWalletAssignedUSDT string
	WalletVerified         bool
}

type ReservationSummary struct {
	Active                 int    `json:"active"`
	PendingModelMarginUSDT string `json:"pending_model_margin_usdt"`
	OwnedModelMarginUSDT   string `json:"owned_model_margin_usdt"`
	AutoWalletBudgetUSDT   string `json:"auto_wallet_budget_usdt"`
}

// OpenCoordinator must be held for the full lifetime of any future order
// manager, NOT just for one reservation call. An orphan directory blocks new
// attempts rather than guessing whether a still-running process owns it.
func OpenCoordinator(dataDir string) (*Coordinator, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 디렉터리가 필요합니다")
	}
	// Coordinate startup with the entire wallet audit+commit interval.
	gate, err := AcquireMutationGate(dataDir)
	if err != nil {
		return nil, err
	}
	defer gate.Close() // failure is fail-closed; retained lock blocks future operations
	root := filepath.Join(dataDir, "trading")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock := filepath.Join(root, "coordinator.active")
	if err := os.Mkdir(lock, 0700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, ErrCoordinatorLocked
		}
		return nil, err
	}
	// If anything fails, deliberately retain the lock for inspection. An
	// empty lock directory after a crash is still evidence of uncertainty.
	nonceRaw := make([]byte, 24)
	if _, err := rand.Read(nonceRaw); err != nil {
		return nil, err
	}
	nonce := hex.EncodeToString(nonceRaw)
	marker, err := os.OpenFile(filepath.Join(lock, "owner.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]any{"schema": 1, "nonce": nonce, "pid": os.Getpid(), "at": time.Now().UTC().Format(time.RFC3339Nano)})
	_, err = marker.Write(append(body, '\n'))
	if err == nil {
		err = marker.Sync()
	}
	closeErr := marker.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "reservations")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	c := &Coordinator{root: root, lockDir: lock, reserveDir: dir, nonce: nonce}
	// A corrupt reservation is NOT silently skipped when the coordinator
	// starts; fail locked rather than ignoring possible active exposure.
	if _, err := c.scan(); err != nil {
		return nil, err
	}
	if _, err := ReviewExecutionLifecycles(dataDir); err != nil {
		return nil, fmt.Errorf("주문 실행 lifecycle 포렌식 검증 실패: %w", err)
	}
	return c, nil
}

func (c *Coordinator) ownsLock() error {
	if c == nil || c.closed {
		return errors.New("거래 조정기가 종료되었습니다")
	}
	raw, err := os.ReadFile(filepath.Join(c.lockDir, "owner.json"))
	if err != nil {
		return err
	}
	var marker struct {
		Schema int    `json:"schema"`
		Nonce  string `json:"nonce"`
	}
	if err := json.Unmarshal(raw, &marker); err != nil || marker.Schema != 1 || marker.Nonce != c.nonce {
		return errors.New("거래 조정기 소유권이 일치하지 않습니다")
	}
	return nil
}

func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ownsLock(); err != nil {
		return err
	}
	// No network action occurs here. Active reservations must survive clean
	// restart too; Close releases ONLY exclusive process ownership.
	if err := os.Remove(filepath.Join(c.lockDir, "owner.json")); err != nil {
		return err
	}
	if err := os.Remove(c.lockDir); err != nil {
		return err
	}
	c.closed = true
	return nil
}

func decodeReservation(path string) (Reservation, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Reservation{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var r Reservation
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Reservation{}, errors.New("예약 파일에 뒤따르는 데이터가 있습니다")
	}
	if r.Schema != 1 || !reservationName.MatchString(filepath.Base(path)) ||
		filepath.Base(path) != r.ClientOrderID+".json" || r.Checksum == "" || r.Checksum != r.digest() {
		return Reservation{}, errors.New("예약 기록이 손상되었습니다")
	}
	if p, err := r.Intent.Plan(); err != nil || p.ClientOrderID != r.ClientOrderID {
		return Reservation{}, errors.New("예약 주문 내용 불일치")
	}
	digest, err := digestIntent(r.Intent)
	if err != nil || digest != r.IntentDigest {
		return Reservation{}, errors.New("예약 요청 해시 불일치")
	}
	if _, err := pos(r.ReservedQuantity); err != nil {
		return Reservation{}, err
	}
	if _, err := decMargin(r.MarginUSDT); err != nil {
		return Reservation{}, err
	}
	return r, nil
}
func decMargin(s string) (*big.Rat, error) { return dec(s) }

// Round POSITIVE collateral reservations upward. FloatString rounds to nearest
// and could understate margin; truncation is never used for risk accounting.
func ceilingDecimal12(x *big.Rat) string {
	units := new(big.Rat).Mul(x, new(big.Rat).SetInt64(1000000000000))
	q, rem := new(big.Int).QuoRem(units.Num(), units.Denom(), new(big.Int))
	if rem.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	out := new(big.Rat).SetFrac(q, big.NewInt(1000000000000))
	return ratToString(out)
}

func (c *Coordinator) scan() ([]Reservation, error) {
	files, err := os.ReadDir(c.reserveDir)
	if err != nil {
		return nil, err
	}
	out := make([]Reservation, 0, len(files))
	known := make(map[string]Reservation)
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("알 수 없는 예약 하위 디렉터리 또는 심볼릭 링크")
		}
		name := f.Name()
		if strings.HasSuffix(name, ".done.json") {
			base := strings.TrimSuffix(name, ".done.json") + ".json"
			if !reservationName.MatchString(base) {
				return nil, errors.New("비정상 예약 종료 파일")
			}
			continue
		}
		if !reservationName.MatchString(name) {
			return nil, fmt.Errorf("비정상 예약 파일: %s", name)
		}
		r, err := decodeReservation(filepath.Join(c.reserveDir, name))
		if err != nil {
			return nil, err
		}
		known[name] = r
		out = append(out, r)
	}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".done.json") {
			continue
		}
		base := strings.TrimSuffix(f.Name(), ".done.json") + ".json"
		r, ok := known[base]
		if !ok {
			return nil, errors.New("원본이 없는 예약 종료 기록")
		}
		if _, err := decodeReservationDone(filepath.Join(c.reserveDir, f.Name()), r); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientOrderID < out[j].ClientOrderID })
	return out, nil
}
func (c *Coordinator) isDone(r Reservation) bool {
	_, err := os.Stat(filepath.Join(c.reserveDir, r.ClientOrderID+".done.json"))
	return err == nil // scan() already ensured correctness and no unreadable files
}

// enforcePooledSideSerialization uses the durable reservation ledger as the
// side fence. An active EXIT blocks every subsequent mutation on that Hedge
// side. Starting an EXIT also requires all older same-side entries to have
// reached Settle(), which itself requires fresh signed order/fill/position
// reconciliation. Opposite Hedge sides remain independent.
func (c *Coordinator) enforcePooledSideSerialization(i Intent, all []Reservation) error {
	for _, r := range all {
		// dev.3.13 lifecycle records are stronger than the legacy done marker.
		// If Settle() fsync'd its done marker and the process died before the
		// lifecycle SETTLED sidecar, keep that Hedge side fenced after restart.
		stage, hasLifecycle, lifecycleErr := executionLifecycleStatusForReservation(filepath.Dir(c.root), r)
		if lifecycleErr != nil {
			return fmt.Errorf("주문 실행 lifecycle 검증 실패 (%s): %w", r.ClientOrderID, lifecycleErr)
		}
		// Once a network side effect MAY have happened, account collateral and
		// exposure are globally uncertain until the complete lifecycle settles.
		// This is a stronger global risk gate than the dev.3.12 same-side fence.
		if hasLifecycle && stage != ExecutionReserved && stage != ExecutionSettled {
			return fmt.Errorf("%w: active=%s stage=%s owner=%s/%s", ErrExecutionLifecyclePending, r.ClientOrderID, stage, r.Intent.Source, r.Intent.ModelID)
		}
		if r.Intent.PositionSide != i.PositionSide {
			continue
		}
		if hasLifecycle && stage == ExecutionReserved {
			// A RESERVED lifecycle has no exchange side effect yet; retain the
			// original pooled-side reservation semantics below.
		}
		if c.isDone(r) {
			continue
		}
		if r.Intent.Action == Exit {
			return fmt.Errorf("%w: active=%s owner=%s/%s", ErrPooledSideExitInFlight, r.ClientOrderID, r.Intent.Source, r.Intent.ModelID)
		}
		if i.Action == Exit {
			return fmt.Errorf("%w: active=%s owner=%s/%s", ErrPooledSideMutationPending, r.ClientOrderID, r.Intent.Source, r.Intent.ModelID)
		}
	}
	return nil
}
func validBasis(b ReservationBasis, now time.Time) error {
	s := b.Snapshot
	if !b.Audit.ReadyForAssessment || len(b.Audit.Blockers) != 0 || s.Status != "connected" || s.Environment != "demo" ||
		s.Positions == nil || s.Balances == nil || s.Orders == nil || s.AlgoOrders == nil || len(s.AlgoOrders) != 0 || len(s.Orders) != 0 {
		return errors.New("완전히 검증된 최신 Demo 계정/체결 장부가 필요합니다")
	}
	t, err := time.Parse(time.RFC3339Nano, s.AsOf)
	if err != nil || t.After(now.Add(time.Second)) || now.Sub(t) > 5*time.Second {
		return errors.New("계정 데이터가 오래되었습니다")
	}
	if b.Quote.Symbol != "BTCUSDT" || b.Quote.ReceivedAt.IsZero() || b.Quote.ReceivedAt.After(now.Add(time.Second)) || now.Sub(b.Quote.ReceivedAt) > 3*time.Second {
		return errors.New("실시간 가격이 오래되었습니다")
	}
	if _, err := pos(b.Quote.Price); err != nil {
		return errors.New("유효한 시장 가격이 아닙니다")
	}
	return nil
}
func collateral(s exchange.Snapshot) (*big.Rat, *big.Rat, error) {
	var wallet, available *big.Rat
	for _, b := range s.Balances {
		if b.Asset != "USDT" {
			continue
		}
		if wallet != nil {
			return nil, nil, errors.New("중복 USDT 잔액")
		}
		var err error
		wallet, err = pos(b.Wallet)
		if err != nil {
			return nil, nil, err
		}
		available, err = dec(b.Available)
		if err != nil {
			return nil, nil, err
		}
		if available.Cmp(wallet) > 0 {
			return nil, nil, errors.New("가용 자산이 지갑 잔액을 초과합니다")
		}
	}
	if wallet == nil {
		return nil, nil, errors.New("USDT 자산 데이터가 없습니다")
	}
	return wallet, available, nil
}

// ReserveAudited is the only exported reservation API. The journal and the
// imported fill ledger MUST live under the same dataDir as this coordinator.
// Orders and snapshot must have been independently signed-read from the
// selected Demo account; this method recomputes the audit rather than trusting
// an externally supplied ReadyForAssessment flag. Still NOT a live-order
// authorization token: external flat trades and cross-symbol equity require
// a user-data baseline, stream and full-portfolio risk implementation.
func (c *Coordinator) ReserveAudited(i Intent, store *OwnershipStore, snapshot exchange.Snapshot, orders map[string]OrderObservation, quote Quote, now time.Time) (Reservation, ReservationSummary, error) {
	if c == nil || store == nil || filepath.Clean(store.journalDir) != filepath.Join(c.root, "requests") || filepath.Clean(store.fillDir) != filepath.Join(c.root, "fills") {
		return Reservation{}, ReservationSummary{}, errors.New("동일한 데이터 디렉터리의 검증된 체결 장부가 필요합니다")
	}
	report, err := store.Audit(snapshot, orders, now)
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	return c.reservePreview(i, ReservationBasis{Snapshot: snapshot, Audit: report, Quote: quote}, now)
}

// ReserveAuditedWithWallet is an INTERNAL preview adapter, not an execution
// permit. Its allocation can only originate from the matching durable store.
func (c *Coordinator) ReserveAuditedWithWallet(i Intent, store *OwnershipStore, budget *walletpkg.Store, accountFingerprint string, snapshot exchange.Snapshot, orders map[string]OrderObservation, quote Quote, now time.Time) (Reservation, ReservationSummary, error) {
	if c == nil || budget == nil || !budget.BelongsTo(filepath.Dir(c.root)) {
		return Reservation{}, ReservationSummary{}, errors.New("자동매매 배정 지갑 경로 불일치")
	}
	if c == nil || store == nil || filepath.Clean(store.journalDir) != filepath.Join(c.root, "requests") || filepath.Clean(store.fillDir) != filepath.Join(c.root, "fills") {
		return Reservation{}, ReservationSummary{}, errors.New("장부 경로 불일치")
	}
	a, err := budget.SnapshotFor(accountFingerprint)
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	report, err := store.Audit(snapshot, orders, now)
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	return c.reservePreview(i, ReservationBasis{Snapshot: snapshot, Audit: report, Quote: quote, AutoWalletAssignedUSDT: a.AssignedUSDT, WalletVerified: a.Verified}, now)
}

// reservePreview exercises exact arithmetic in LOCAL MOCK TESTS only. No UI
// caller or production trading path must ever construct an audit result itself.
func (c *Coordinator) reservePreview(i Intent, b ReservationBasis, now time.Time) (Reservation, ReservationSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ownsLock(); err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	plan, err := i.Plan()
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	digest, _ := digestIntent(i)
	all, err := c.scan()
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	// Duplicate comparison must occur BEFORE evaluating new balance data.
	for _, r := range all {
		if r.ClientOrderID == plan.ClientOrderID {
			if r.IntentDigest != digest || r.Intent != i {
				return Reservation{}, ReservationSummary{}, errors.New("예약 요청 ID가 다른 주문에 재사용되었습니다")
			}
			return r, ReservationSummary{}, errors.New("이미 예약된 요청입니다. 거래소 확인 없이 재예약할 수 없습니다")
		}
	}
	if err := c.enforcePooledSideSerialization(i, all); err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	if err := validBasis(b, now); err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	wallet, available, err := collateral(b.Snapshot)
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	qty, _ := pos(i.Quantity)
	quote, _ := pos(b.Quote.Price)
	price := quote
	if i.OrderType == Limit {
		limit, err := pos(i.Price)
		if err != nil {
			return Reservation{}, ReservationSummary{}, err
		}
		if limit.Cmp(price) > 0 {
			price = limit
		} // conservative collateral for BUY; used only as conservative max
	}
	margin := ratDiv(ratMul(qty, price), big.NewRat(15, 1))
	ownedMargin := new(big.Rat)
	ownedQty := new(big.Rat)
	for _, p := range b.Audit.Owned {
		if p.Symbol != "BTCUSDT" {
			return Reservation{}, ReservationSummary{}, errors.New("지원하지 않는 장부 종목")
		}
		n, err := dec(p.Quantity)
		if err != nil {
			return Reservation{}, ReservationSummary{}, err
		}
		if p.Source == Model && n.Sign() > 0 {
			avg, err := pos(p.AverageEntry)
			if err != nil {
				return Reservation{}, ReservationSummary{}, err
			}
			ownedMargin.Add(ownedMargin, ratDiv(ratMul(n, avg), big.NewRat(15, 1)))
		}
		if p.Source == i.Source && p.ModelID == i.ModelID && p.Side == i.PositionSide {
			ownedQty.Add(ownedQty, n)
		}
	}
	reservedModelMargin := new(big.Rat)
	reservedManualMargin := new(big.Rat)
	reservedExit := new(big.Rat)
	for _, r := range all {
		if c.isDone(r) {
			continue
		}
		v, _ := dec(r.MarginUSDT)
		if r.Intent.Action == Enter {
			if r.Intent.Source == Model {
				reservedModelMargin.Add(reservedModelMargin, v)
			} else {
				reservedManualMargin.Add(reservedManualMargin, v)
			}
		}
		if r.Intent.Action == Exit && r.Intent.Source == i.Source && r.Intent.ModelID == i.ModelID && r.Intent.PositionSide == i.PositionSide {
			n, _ := pos(r.ReservedQuantity)
			reservedExit.Add(reservedExit, n)
		}
	}
	_ = wallet // The physical wallet is NOT the automatic allocation budget.
	if i.Source == Model && i.Action == Enter && (!b.WalletVerified || b.AutoWalletAssignedUSDT == "") {
		return Reservation{}, ReservationSummary{}, errors.New("자동매매 배정 지갑 및 계정 감사 검증 전 주문 예약 불가")
	}
	cap := new(big.Rat)
	if b.AutoWalletAssignedUSDT != "" {
		var err error
		cap, err = walletDecimal(b.AutoWalletAssignedUSDT)
		if err != nil {
			return Reservation{}, ReservationSummary{}, err
		}
	}
	if i.Action == Enter {
		if i.Source == Model {
			used := new(big.Rat).Add(reservedModelMargin, ownedMargin)
			assessment, err := walletpkg.Assess(b.AutoWalletAssignedUSDT, ceilingDecimal12(ownedMargin), ratToString(reservedModelMargin), ceilingDecimal12(margin), ratToString(available))
			if err != nil || !assessment.Fits {
				return Reservation{}, ReservationSummary{}, errors.New("자동매매 배정 지갑의 가용 한도 또는 신규 진입 10% 한도 초과")
			}
			if new(big.Rat).Add(used, margin).Cmp(cap) > 0 {
				return Reservation{}, ReservationSummary{}, errors.New("자동매매 배정 예산 부족")
			}
		}
		if new(big.Rat).Add(margin, new(big.Rat).Add(reservedModelMargin, reservedManualMargin)).Cmp(available) > 0 {
			return Reservation{}, ReservationSummary{}, errors.New("신규 주문의 예약 증거금이 가용 USDT를 초과합니다")
		}
	} else {
		if ownedQty.Sub(ownedQty, reservedExit).Cmp(qty) < 0 {
			return Reservation{}, ReservationSummary{}, errors.New("해당 소유자의 미예약 포지션 수량이 부족합니다")
		}
		margin.SetInt64(0) // exits consume owner quantity, not new collateral
	}
	r := Reservation{Schema: 1, ClientOrderID: plan.ClientOrderID, Intent: i, IntentDigest: digest, ReservedQuantity: i.Quantity, MarginUSDT: ceilingDecimal12(margin), CreatedAt: now.UTC().Format(time.RFC3339Nano)}
	r.seal()
	path := filepath.Join(c.reserveDir, r.ClientOrderID+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Reservation{}, ReservationSummary{}, err
	}
	raw, _ := json.MarshalIndent(r, "", "  ")
	_, err = f.Write(append(raw, '\n'))
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return Reservation{}, ReservationSummary{}, fmt.Errorf("예약 디스크 기록 실패, 수동 검사 필요: %w", err)
	}
	return r, ReservationSummary{Active: 1, PendingModelMarginUSDT: ratToString(reservedModelMargin), OwnedModelMarginUSDT: ratToString(ownedMargin), AutoWalletBudgetUSDT: ratToString(cap)}, nil
}

// Settle may only release a reservation AFTER an independent exchange audit
// has reconciled all fills and found no open/unresolved orders. Do not infer
// completion from a cancel ACK, an expired timer, or local UI state.
func (c *Coordinator) Settle(r Reservation, store *OwnershipStore, snapshot exchange.Snapshot, orders map[string]OrderObservation, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ownsLock(); err != nil {
		return err
	}
	if store == nil {
		return errors.New("체결 장부가 필요합니다")
	}
	all, err := c.scan()
	if err != nil {
		return err
	}
	var original *Reservation
	for k := range all {
		if all[k].ClientOrderID == r.ClientOrderID {
			original = &all[k]
		}
	}
	if original == nil || *original != r {
		return errors.New("예약 내용이 실제 저장 내용과 다릅니다")
	}
	// A dev.3.13 execution lifecycle, when present, must already have reached
	// WHOLE_SIDE_RECONCILED before the reservation may be settled. Legacy
	// reservations without a lifecycle remain readable for cumulative upgrade.
	lifecycleStage, hasLifecycle, lifecycleErr := executionLifecycleStatusForReservation(filepath.Dir(c.root), r)
	if lifecycleErr != nil {
		return fmt.Errorf("주문 실행 lifecycle 검증 실패: %w", lifecycleErr)
	}
	if hasLifecycle && lifecycleStage != ExecutionWholeReconciled && lifecycleStage != ExecutionSettled {
		return fmt.Errorf("%w: current=%s", ErrExecutionStageOrder, lifecycleStage)
	}
	if c.isDone(r) {
		return nil
	}
	report, err := store.Audit(snapshot, orders, now)
	if err != nil || !report.ReadyForAssessment {
		return errors.New("거래소/체결 장부 감사 완료 전에는 예약을 해제할 수 없습니다")
	}
	obs, ok := orders[r.ClientOrderID]
	if !ok {
		return errors.New("종료를 증명하는 거래소 주문 정보가 없습니다")
	}
	switch obs.Order.Status {
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
	default:
		return errors.New("해당 주문이 종료되지 않았습니다")
	}
	if obs.Order.ClientOrderID != r.ClientOrderID {
		return errors.New("거래소 주문 ID 불일치")
	}
	// The audit above independently verifies origQty, executedQty and all
	// persisted fills against actual account positions. Persist enough sealed
	// evidence to prove after a restart that a same-side fence was released
	// only after whole-position reconciliation, without storing credentials.
	ownerRemaining := new(big.Rat)
	for _, p := range report.Owned {
		if p.Source == r.Intent.Source && p.ModelID == r.Intent.ModelID && p.Symbol == r.Intent.Symbol && p.Side == r.Intent.PositionSide {
			q, e := signedRat(p.Quantity)
			if e != nil || q.Sign() < 0 {
				return errors.New("감사 후 소유자 수량 오류")
			}
			ownerRemaining.Add(ownerRemaining, q)
		}
	}
	exchangeSide := new(big.Rat)
	for _, p := range snapshot.Positions {
		if p.Symbol != r.Intent.Symbol || p.Side != string(r.Intent.PositionSide) {
			continue
		}
		q, e := signedRat(p.Qty)
		if e != nil {
			return errors.New("감사 후 거래소 포지션 수량 오류")
		}
		q.Abs(q)
		exchangeSide.Add(exchangeSide, q)
	}
	done := reservationDoneMarker{Schema: 2, ClientOrderID: r.ClientOrderID, ReservationChecksum: r.Checksum,
		SettledAt: now.UTC().Format(time.RFC3339Nano), Action: r.Intent.Action, PositionSide: r.Intent.PositionSide,
		SignedSnapshotAsOf: snapshot.AsOf, OwnerRemainingQty: formatOwnershipDecimal(ownerRemaining),
		ExchangeSideQty: formatOwnershipDecimal(exchangeSide), OwnershipReady: true}
	done.Checksum = done.digest()
	f, err := os.OpenFile(filepath.Join(c.reserveDir, r.ClientOrderID+".done.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(done, "", "  ")
	_, err = f.Write(append(raw, '\n'))
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return fmt.Errorf("예약 해제 기록 저장 실패: %w", err)
	}
	return nil
}

func walletDecimal(s string) (*big.Rat, error) { return walletpkg.Decimal(s, true) }
