package main

// Signed external-close REVALIDATION. Every network call is a Demo-only GET.
// Probe writes a report in pc-results; stage/recheck (explicit opt-in) also
// write append-only NOT-POSTED sidecar records. Nothing posts to owner journals.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

type replayResult struct {
	Schema            int                                `json:"schema"`
	Stage             string                             `json:"stage"`
	ProposalSHA256    string                             `json:"proposal_sha256"`
	Revalidation      trading.ExternalCloseRevalidation  `json:"revalidation"`
	Checkpoint        *trading.ExternalCloseReplayRecord `json:"checkpoint,omitempty"`
	PostingAuthorized bool                               `json:"posting_authorized"`
	TradingAuthorized bool                               `json:"trading_authorized"`
}

func runSignedReplay(root, output, action, clientID, confirm string) error {
	if action != "probe" && action != "stage" && action != "recheck" {
		return errors.New("replay action must be probe | stage | recheck")
	}
	if action != "probe" && confirm != "CHECKPOINT_ONLY_NOT_SETTLEMENT" {
		return errors.New("explicit non-posting checkpoint confirmation required")
	}
	out, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "pc-results")
	if filepath.Dir(out) != dir || filepath.Ext(out) != ".json" {
		return errors.New("report must be direct .json child of pc-results")
	}
	if fi, e := os.Lstat(dir); e == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return errors.New("unsafe report directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	s, err := trading.NewExternalCloseSettlementProposalStore(filepath.Join(root, "data"))
	if err != nil {
		return err
	}
	p, err := s.Load(clientID)
	if err != nil {
		return err
	}
	creds, err := readCredentials(root)
	if err != nil {
		return fmt.Errorf("invalid local Demo credential file: %w", err)
	}
	reader := trading.DemoSignedCloseReader{Client: exchange.NewDemoClient(), Key: creds.APIKey, Secret: creds.Secret, Now: time.Now}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	now := time.Now().UTC()
	verified, err := trading.RevalidateExternalClose(ctx, p, reader, now)
	if err != nil {
		return err
	}
	if err = trading.ValidateExternalCloseRevalidation(verified); err != nil {
		return err
	}
	result := replayResult{Schema: 1, Stage: "SIGNED_REVALIDATION_NOT_POSTED", ProposalSHA256: p.Checksum, Revalidation: verified}
	if action != "probe" {
		store, err := trading.NewExternalCloseReplayCheckpointStore(filepath.Join(root, "data"))
		if err != nil {
			return err
		}
		var checkpoint trading.ExternalCloseReplayRecord
		if action == "stage" {
			checkpoint, err = store.Stage(ctx, clientID, reader, time.Now().UTC())
		} else {
			checkpoint, err = store.Recheck(ctx, clientID, reader, time.Now().UTC())
		}
		if err != nil {
			return err
		}
		result.Checkpoint = &checkpoint
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if _, err = file.Write(append(b, '\n')); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	fmt.Printf("SIGNED DEMO REVALIDATION (NOT POSTED): %s; order submissions: 0; trading authorized: false\n", out)
	return nil
}
