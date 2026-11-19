// dev.3.130 offline signature-bound non-posting reviewer receipt.
// This tool has NO exchange client, signed REST, private credentials,
// financial posting, order placement, or production trading route.
package main

import (
	"binance_trader/internal/trading"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type manifest struct {
	Schema int `json:"schema"`
	Events []struct {
		Data         string `json:"data"`
		ID           string `json:"id"`
		IncidentFile string `json:"incident"`
	} `json:"events"`
}

func readBoundedJSON(path string, max int64, out any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > max {
		return errors.New("invalid bounded regular JSON file")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, max+1))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(out); err != nil {
		return err
	}
	var tail any
	if err = decoder.Decode(&tail); err != io.EOF {
		return errors.New("extra or malformed JSON")
	}
	return nil
}
func readEvents(path string) ([]trading.ExternalFinancialEventSource, error) {
	var m manifest
	if err := readBoundedJSON(path, 1<<20, &m); err != nil {
		return nil, err
	}
	if m.Schema != 1 || len(m.Events) < 2 || len(m.Events) > 64 {
		return nil, errors.New("manifest schema 1 with 2..64 events required")
	}
	events := make([]trading.ExternalFinancialEventSource, 0, len(m.Events))
	for _, e := range m.Events {
		if e.Data == "" || e.ID == "" || e.IncidentFile == "" {
			return nil, errors.New("manifest data/id/incident required")
		}
		var incident trading.PackageAIncidentArchive
		if err := readBoundedJSON(e.IncidentFile, 8<<20, &incident); err != nil {
			return nil, fmt.Errorf("incident %s: %w", e.ID, err)
		}
		events = append(events, trading.ExternalFinancialEventSource{DataDir: e.Data, ClientOrderID: e.ID, Incident: incident})
	}
	return events, nil
}
func run(ctx context.Context, action, registryRoot, manifestPath, requestPath, attestPath, keyPath, pin, receiptRoot string, nowUnix int64, out io.Writer) error {
	if registryRoot == "" || manifestPath == "" || ctx == nil {
		return errors.New("existing registry and exact incident manifest required")
	}
	registry, err := trading.NewExternalGlobalEvidenceReservationStore(registryRoot)
	if err != nil {
		return err
	}
	events, err := readEvents(manifestPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	switch action {
	case "intent":
		if requestPath != "" || attestPath != "" || keyPath != "" || pin != "" || receiptRoot != "" {
			return errors.New("intent never accepts attestation or receipt parameters")
		}
		state, err := trading.InspectExternalGlobalAtomicConsistency(ctx, registry, events)
		if err != nil {
			return err
		}
		nonce := make([]byte, 32)
		if _, err = rand.Read(nonce); err != nil {
			return err
		}
		request, err := trading.PrepareExternalIndependentApprovalRequest(state, hex.EncodeToString(nonce), nowUnix, nowUnix+3600)
		if err != nil {
			return err
		}
		return enc.Encode(request)
	case "verify-once":
		if requestPath == "" || attestPath == "" || keyPath == "" || pin == "" || receiptRoot == "" {
			return errors.New("verify-once requires signed attestation, request, independent pinned key and separate receipt root")
		}
		var request trading.ExternalIndependentApprovalRequest
		if err := readBoundedJSON(requestPath, 128<<10, &request); err != nil {
			return err
		}
		var attestation trading.ExternalIndependentReviewAttestation
		if err := readBoundedJSON(attestPath, 16<<10, &attestation); err != nil {
			return err
		}
		// Never trust a public key embedded in a request/attestation. The
		// reviewer key path AND its hash must be pinned out of band.
		stat, err := os.Lstat(keyPath)
		if err != nil {
			return err
		}
		if !stat.Mode().IsRegular() || stat.Size() < 64 || stat.Size() > 512 {
			return errors.New("unsafe independent reviewer public-key file")
		}
		raw, err := os.ReadFile(keyPath)
		if err != nil {
			return err
		}
		serialized := strings.TrimSpace(string(raw))
		key, err := hex.DecodeString(serialized)
		if err != nil || len(key) != ed25519.PublicKeySize || hex.EncodeToString(key) != serialized {
			return errors.New("public key must be 32 bytes lowercase hex")
		}
		checksum := sha256.Sum256(key)
		if hex.EncodeToString(checksum[:]) != pin {
			return errors.New("public key does not match independently pinned SHA256")
		}
		receipts, err := trading.NewExternalIndependentReviewReceiptStore(receiptRoot)
		if err != nil {
			return err
		}
		report, err := receipts.RecordVerifiedExternalIndependentReview(ctx, registry, events, request, attestation, key, pin, nowUnix)
		if err != nil {
			return err
		}
		return enc.Encode(report)
	default:
		return errors.New("action must be intent or verify-once")
	}
}
func main() {
	action := flag.String("action", "intent", "intent|verify-once (always non-posting)")
	root := flag.String("registry", "", "existing offline global reservation root")
	manifest := flag.String("manifest", "", "original 2..64 event manifest")
	req := flag.String("request", "", "unsigned exact batch review request JSON")
	att := flag.String("attestation", "", "externally signed reviewer acknowledgement JSON")
	key := flag.String("reviewer-pubkey", "", "independently trusted Ed25519 public key file, lowercase hex")
	pin := flag.String("pin-sha256", "", "out-of-band trusted SHA256 of reviewer public key, lowercase hex")
	receipt := flag.String("receipt-root", "", "existing independent nonce receipt directory")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "BLOCKED: unexpected positional arguments")
		os.Exit(1)
	}
	if err := run(context.Background(), *action, *root, *manifest, *req, *att, *key, *pin, *receipt, time.Now().Unix(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
}
