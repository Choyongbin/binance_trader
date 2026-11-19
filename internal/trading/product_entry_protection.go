package trading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
)

// productProtectionPlanner only creates a local, non-executable plan after
// signed fill recovery. It cannot arm or cancel an exchange Algo order.
func productProtectionPlanner(dataDir string, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	ownership *OwnershipStore, client *exchange.Client, credentials CredentialsProvider, market AutoMarketProvider,
) func(context.Context, Reservation) (RecoveredEntryProtectionReview, error) {
	return func(requestCtx context.Context, reservation Reservation) (RecoveredEntryProtectionReview, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), 12*time.Second)
		defer cancel()
		creds, err := credentials(ctx)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		live, err := market(ctx)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		contract, err := quoteFromLive(live)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		account, err := client.Fetch(ctx, creds.APIKey, creds.Secret)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		if account.Environment != "demo" || account.Status != "connected" {
			return RecoveredEntryProtectionReview{}, errors.New("signed Demo account unavailable for protection preview")
		}
		asOf, err := time.Parse(time.RFC3339Nano, account.AsOf)
		if err != nil || time.Since(asOf) < 0 || time.Since(asOf) > 3*time.Second {
			return RecoveredEntryProtectionReview{}, errors.New("signed account mark price is stale or has no timestamp")
		}
		var mark Quote
		for _, p := range account.Positions {
			if p.Symbol != reservation.Intent.Symbol || p.Side != string(reservation.Intent.PositionSide) {
				continue
			}
			if mark.Symbol != "" {
				return RecoveredEntryProtectionReview{}, errors.New("duplicate signed hedge-side positions")
			}
			if _, err := positiveRat(p.Mark); err != nil {
				return RecoveredEntryProtectionReview{}, fmt.Errorf("invalid signed mark price: %w", err)
			}
			mark = Quote{Symbol: p.Symbol, Price: p.Mark, ReceivedAt: asOf}
		}
		if mark.Symbol == "" {
			return RecoveredEntryProtectionReview{}, errors.New("signed owned position is absent")
		}
		rules, err := client.FetchSymbolRules(ctx, reservation.Intent.Symbol)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		plans, err := OpenProtectionSubmissionStore(dataDir)
		if err != nil {
			return RecoveredEntryProtectionReview{}, err
		}
		return RecoverAndPrepareEntryProtectionSupervised(ctx, reservation, coordinator, lifecycle,
			ownership, plans, client, client, creds, rules, contract, mark)
	}
}
