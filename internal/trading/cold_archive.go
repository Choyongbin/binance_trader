package trading

// Cold diagnostic archives compact historical read-only summaries without
// touching execution/request/fill/protection source-of-truth evidence. A
// bundle is sealed and re-read before any source file is removed. Duplicate
// presence after a crash is safe because loaders deduplicate by original name.

import (
	"bytes"
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

const (
	ColdKindRecovery   = "recovery_review"
	ColdKindIntegrated = "integrated_review"
	ColdKindAccounting = "accounting_report"
)

var coldArchiveName = regexp.MustCompile(`^archive_[0-9]{8}T[0-9]{6}_[0-9a-f]{32}\.json$`)

type ColdArchiveEntry struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name"`
	SHA256 string          `json:"sha256"`
	Raw    json.RawMessage `json:"raw"`
}

type ColdArchiveBundle struct {
	Schema    int                `json:"schema"`
	CreatedAt time.Time          `json:"created_at"`
	Entries   []ColdArchiveEntry `json:"entries"`
	SHA256    string             `json:"sha256"`
}

func (b ColdArchiveBundle) digest() string {
	b.SHA256 = ""
	raw, _ := json.Marshal(b)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func canonicalJSON(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func coldRoot(dataDir string) string { return filepath.Join(dataDir, "trading", "cold_archives") }

func validColdKind(k string) bool {
	return k == ColdKindRecovery || k == ColdKindIntegrated || k == ColdKindAccounting
}

func readColdBundles(dataDir string) ([]ColdArchiveBundle, error) {
	dir := coldRoot(dataDir)
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []ColdArchiveBundle{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]ColdArchiveBundle, 0, len(files))
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !coldArchiveName.MatchString(f.Name()) {
			return nil, errors.New("invalid cold archive entry")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		var b ColdArchiveBundle
		if err := dec.Decode(&b); err != nil {
			return nil, err
		}
		var tail any
		if err := dec.Decode(&tail); !errors.Is(err, io.EOF) {
			return nil, errors.New("cold archive trailing data")
		}
		if b.Schema != 1 || b.CreatedAt.IsZero() || b.SHA256 == "" || b.SHA256 != b.digest() || b.Entries == nil {
			return nil, errors.New("cold archive checksum invalid")
		}
		prev := ""
		for _, e := range b.Entries {
			if !validColdKind(e.Kind) || strings.TrimSpace(e.Name) == "" || len(e.SHA256) != 64 || len(e.Raw) == 0 {
				return nil, errors.New("cold archive entry invalid")
			}
			if _, err := hex.DecodeString(e.SHA256); err != nil {
				return nil, errors.New("cold archive entry digest invalid")
			}
			key := e.Kind + ":" + e.Name
			if key <= prev {
				return nil, errors.New("cold archive entries not strictly sorted")
			}
			prev = key
			canon, err := canonicalJSON(e.Raw)
			if err != nil {
				return nil, errors.New("cold archive raw JSON invalid")
			}
			s := sha256.Sum256(canon)
			if hex.EncodeToString(s[:]) != e.SHA256 {
				return nil, errors.New("cold archive raw digest mismatch")
			}
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].SHA256 < out[j].SHA256
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func coldEntries(dataDir, kind string) (map[string]json.RawMessage, error) {
	bundles, err := readColdBundles(dataDir)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for _, b := range bundles {
		for _, e := range b.Entries {
			if e.Kind == kind {
				if old, ok := out[e.Name]; ok && string(old) != string(e.Raw) {
					return nil, errors.New("conflicting cold archive duplicate")
				}
				out[e.Name] = append(json.RawMessage(nil), e.Raw...)
			}
		}
	}
	return out, nil
}

type DiagnosticRetentionResult struct {
	Schema            int       `json:"schema"`
	CreatedAt         time.Time `json:"created_at"`
	KeepRecent        int       `json:"keep_recent"`
	Archived          int       `json:"archived"`
	Removed           int       `json:"removed"`
	Bundle            string    `json:"bundle,omitempty"`
	TradingAuthorized bool      `json:"trading_authorized"`
}

type retentionSource struct {
	kind, dir string
	match     *regexp.Regexp
}

type retentionCandidate struct {
	name string
	path string
	at   time.Time
	raw  []byte
}

func decodeRetentionJSON(raw []byte, out any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var tail any
	if err := dec.Decode(&tail); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("retention source trailing data")
		}
		return err
	}
	return nil
}

// retentionSourceTime validates the source report before it can be moved to a
// cold bundle and returns the report's semantic observation time. Random file
// IDs are not chronology and must never decide which records are "recent".
func retentionSourceTime(kind, name string, raw []byte) (time.Time, error) {
	switch kind {
	case ColdKindRecovery:
		var e reviewEnvelope
		if err := decodeRetentionJSON(raw, &e); err != nil || !recoveryName.MatchString(name) ||
			e.Schema != 1 || e.Report.Version != 1 || e.Report.ObservedAt.IsZero() || e.Report.TradingAuthorized ||
			!e.Report.ManualReviewRequired || e.Digest == "" || e.Digest != e.sum() {
			return time.Time{}, errors.New("invalid recovery retention source")
		}
		return e.Report.ObservedAt, nil
	case ColdKindIntegrated:
		var r IntegratedReview
		if err := decodeRetentionJSON(raw, &r); err != nil || !integratedReportName.MatchString(name) ||
			r.Schema != 1 || r.ID+".json" != name || r.CheckedAt.IsZero() || r.SHA256 == "" || r.SHA256 != r.digest() ||
			r.TradingAuthorized || r.StreamContinuityProven || !r.ManualReviewRequired || r.Blockers == nil ||
			!fingerprintPattern046.MatchString(r.EvidenceBefore) || !fingerprintPattern046.MatchString(r.EvidenceAfter) ||
			(r.State != "BLOCKED" && r.State != "OBSERVED_FLAT_NOT_CONTINUOUS") {
			return time.Time{}, errors.New("invalid integrated retention source")
		}
		return r.CheckedAt, nil
	case ColdKindAccounting:
		var r AccountingReport
		if err := decodeRetentionJSON(raw, &r); err != nil || !accountingArchivePattern.MatchString(name) ||
			!validateAccountingArchive(r) || r.ID+".json" != name || r.SHA256 == "" || r.SHA256 != r.digest() {
			return time.Time{}, errors.New("invalid accounting retention source")
		}
		return r.CreatedAt, nil
	default:
		return time.Time{}, errors.New("invalid retention source kind")
	}
}

func CompactDiagnosticArchives(dataDir string, keepRecent int, now time.Time) (DiagnosticRetentionResult, error) {
	out := DiagnosticRetentionResult{Schema: 1, CreatedAt: now.UTC(), KeepRecent: keepRecent, TradingAuthorized: false}
	if strings.TrimSpace(dataDir) == "" || keepRecent < 1 || now.IsZero() {
		return out, errors.New("invalid diagnostic retention request")
	}
	sources := []retentionSource{
		{ColdKindRecovery, filepath.Join(dataDir, "trading", "recovery_reviews"), recoveryName},
		{ColdKindIntegrated, filepath.Join(dataDir, "trading", "integrated_reviews"), integratedReportName},
		{ColdKindAccounting, filepath.Join(dataDir, "trading", "accounting_reports"), accountingArchivePattern},
	}
	entries := []ColdArchiveEntry{}
	paths := []string{}
	for _, s := range sources {
		fs, err := os.ReadDir(s.dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return out, err
		}
		candidates := make([]retentionCandidate, 0, len(fs))
		for _, f := range fs {
			if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !s.match.MatchString(f.Name()) {
				return out, errors.New("retention source contains unknown entry")
			}
			p := filepath.Join(s.dir, f.Name())
			raw, err := os.ReadFile(p)
			if err != nil {
				return out, err
			}
			at, err := retentionSourceTime(s.kind, f.Name(), raw)
			if err != nil {
				return out, err
			}
			candidates = append(candidates, retentionCandidate{name: f.Name(), path: p, at: at, raw: raw})
		}
		sort.Slice(candidates, func(i, j int) bool {
			return chronologicalIDLess(candidates[i].at, candidates[i].name, candidates[j].at, candidates[j].name)
		})
		if len(candidates) <= keepRecent {
			continue
		}
		for _, candidate := range candidates[:len(candidates)-keepRecent] {
			canon, err := canonicalJSON(candidate.raw)
			if err != nil {
				return out, errors.New("retention source JSON invalid")
			}
			sum := sha256.Sum256(canon)
			entries = append(entries, ColdArchiveEntry{Kind: s.kind, Name: candidate.name, SHA256: hex.EncodeToString(sum[:]), Raw: json.RawMessage(append([]byte(nil), canon...))})
			paths = append(paths, candidate.path)
		}
	}
	if len(entries) == 0 {
		return out, nil
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Kind == entries[j].Kind {
			return entries[i].Name < entries[j].Name
		}
		return entries[i].Kind < entries[j].Kind
	})
	if err := os.MkdirAll(coldRoot(dataDir), 0700); err != nil {
		return out, err
	}
	var n [16]byte
	if _, err := rand.Read(n[:]); err != nil {
		return out, err
	}
	name := "archive_" + now.UTC().Format("20060102T150405") + "_" + hex.EncodeToString(n[:]) + ".json"
	bundle := ColdArchiveBundle{Schema: 1, CreatedAt: now.UTC(), Entries: entries}
	bundle.SHA256 = bundle.digest()
	body, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return out, err
	}
	path := filepath.Join(coldRoot(dataDir), name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return out, err
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
		return out, fmt.Errorf("cold archive write uncertain: %w", err)
	}
	// Re-read the complete archive before deleting any source. Crash after this
	// point can only leave safe duplicates, never evidence loss.
	if _, err := readColdBundles(dataDir); err != nil {
		return out, err
	}
	removed := 0
	for _, p := range paths {
		if err := os.Remove(p); err != nil {
			return out, fmt.Errorf("cold archive sealed but source cleanup incomplete after %d removals: %w", removed, err)
		}
		removed++
	}
	out.Archived = len(entries)
	out.Removed = removed
	out.Bundle = name
	return out, nil
}
