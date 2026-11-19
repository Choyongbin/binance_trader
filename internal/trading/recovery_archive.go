package trading

// An immutable, checksummed diagnostic archive. Writing a recovery review is
// not a durable proof that future account events were received or reconciled.
import (
	"binance_trader/internal/exchange"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type reviewEnvelope struct {
	Schema int            `json:"schema"`
	Report RecoveryReview `json:"report"`
	Digest string         `json:"sha256"`
}

func (e reviewEnvelope) sum() string {
	e.Digest = ""
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

var recoveryName = regexp.MustCompile(`^review_[0-9]{8}T[0-9]{6}_[0-9a-f]{32}\.json$`)

func ArchiveRecoveryReview(dataDir string, r RecoveryReview) (string, error) {
	if dataDir == "" || r.Version != 1 || r.TradingAuthorized || !r.ManualReviewRequired || r.ObservedAt.IsZero() {
		return "", errors.New("승인되지 않은 복구 진단 형식")
	}
	dir := filepath.Join(dataDir, "trading", "recovery_reviews")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	name := "review_" + r.ObservedAt.UTC().Format("20060102T150405") + "_" + hex.EncodeToString(nonce[:]) + ".json"
	envelope := reviewEnvelope{Schema: 1, Report: r}
	envelope.Digest = envelope.sum()
	body, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	_, err = f.Write(append(body, '\n'))
	if err == nil {
		err = f.Sync()
	}
	cerr := f.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return "", fmt.Errorf("복구 진단 저장 불확실: %w", err)
	}
	return path, nil
}
func LoadRecoveryReviews(dataDir string) ([]RecoveryReview, error) {
	dir := filepath.Join(dataDir, "trading", "recovery_reviews")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []RecoveryReview{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]RecoveryReview, 0, len(entries))
	seen := map[string]bool{}
	for _, f := range entries {
		if f.IsDir() || !recoveryName.MatchString(f.Name()) {
			return nil, errors.New("복구 진단 파일 형식 오류")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		var e reviewEnvelope
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, errors.New("복구 진단 파일 뒤 추가 데이터")
		}
		if e.Schema != 1 || e.Report.Version != 1 || e.Report.TradingAuthorized || !e.Report.ManualReviewRequired || e.Digest == "" || e.Digest != e.sum() {
			return nil, errors.New("복구 진단 체크섬 또는 거래 잠금 정책 오류")
		}
		seen[f.Name()] = true
		out = append(out, e.Report)
	}
	cold, err := coldEntries(dataDir, ColdKindRecovery)
	if err != nil {
		return nil, err
	}
	for name, raw := range cold {
		if seen[name] {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		var e reviewEnvelope
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, errors.New("복구 진단 cold archive 후행 데이터")
		}
		if !recoveryName.MatchString(name) || e.Schema != 1 || e.Report.Version != 1 || e.Report.TradingAuthorized || !e.Report.ManualReviewRequired || e.Digest == "" || e.Digest != e.sum() {
			return nil, errors.New("복구 진단 cold archive 손상")
		}
		out = append(out, e.Report)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ObservedAt.Before(out[j].ObservedAt) })
	return out, nil
}

type DemoRecoveryReader interface {
	FetchDemoRecoveryBaseline(ctx context.Context, key, secret string, sinceMS int64) (exchange.RecoveryBaseline, error)
}

// Deliberately no automatic startup or reconnect hook. A future operator-
// controlled orchestration layer may call this after the private websocket
// becomes continuous and a complete initial audit has been demonstrated.
func CollectAndArchiveDemoReview(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64) (RecoveryReview, string, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return RecoveryReview{}, "", errors.New("서명된 Demo 복구 조회만 허용")
	}
	b, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, sinceMS)
	if err != nil {
		return RecoveryReview{}, "", err
	}
	store, err := NewOwnershipStore(dataDir)
	if err != nil {
		return RecoveryReview{}, "", err
	}
	now := time.Now().UTC()
	review, err := ReviewRecovery(dataDir, store, b, now)
	if err != nil {
		return RecoveryReview{}, "", err
	}
	path, err := ArchiveRecoveryReview(dataDir, review)
	return review, path, err
}
