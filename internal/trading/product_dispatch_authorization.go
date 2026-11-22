package trading

import (
	"context"
	"errors"
	"fmt"
)

// requireProductProtectionDispatchApproval is a product-only, fail-closed
// authorization boundary. It is deliberately installed by the real Demo
// runtime constructors, not by the injectable test-only execution path.
//
// At present we cannot prove continuous protection, safe sibling cancellation,
// and account-wide ownership. Consequently *no* model entry OR exit may reach
// the exchange dispatcher through the product runtime. Successful signed
// preflight, a local protection plan, and HTTP route authentication are not a
// substitute for an explicit, independently reviewed trading approval.
//
// When a verified supervised product lifecycle exists, replace this boundary
// with a durable capability tied to one account, position, order, and run
// generation. Do not change this to a caller-controlled bool or JSON flag.
func requireProductProtectionDispatchApproval(next func(context.Context, Intent) error) func(context.Context, Intent) error {
	return func(ctx context.Context, intent Intent) error {
		if ctx == nil {
			return errors.New("Demo product protection dispatch context is nil")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Conservatively block unknown/malformed actions as well. There is
		// no permitted mutation at this boundary before product certification.
		// Keep ErrProtectionApproval so callers cannot misreport this as a
		// signed-account transient error that could be retried.
		_ = next
		return fmt.Errorf("Demo product %s %s dispatch denied until protection supervision is certified: %w", intent.Symbol, intent.Action, ErrProtectionApproval)
	}
}
