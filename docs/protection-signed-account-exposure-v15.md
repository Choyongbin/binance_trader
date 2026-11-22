# v15 — Signed account-wide Hedge exposure review (non-authorizing)

This incremental step does **not** enable automated trading, protection submission,
protection cancellation, automatic replay, fence release, financial settlement, or HTTP routes.
The Binance Demo product `Arm` and `CancelTriggeredSibling` remain blocked by
`ErrProtectionApproval`; HTTP START/STOP trading routes stay HTTP 423.

## Wiring

`DemoProtectionRuntimeCoordinator.InspectProductSignedAccountExposureV15(ctx, fromMS)`
requires the existing exclusive coordinator lease, immutable saved protection
plan, precisely one persistent unreconciled mutation fence, account-key SHA-256
fingerprint match, and the bounded signed history start time.

The method calls the existing Binance Demo `FetchDemoRecoveryBaseline` **twice**;
each call uses actual signed GET for account-wide portfolio, account config,
open regular/Algo orders, and bounded recent orders, trades and income. It
runs `ReviewRecovery` on each separately against the local durable journal,
fill/ownership records and protection history. It has no POST/DELETE API calls.

`InspectSignedAccountFramePairV15` requires fresh, non-overlapping, stable
account-wide observations, identical canonical evidence digests, no foreign
exposure or non-USDT collateral, no open standard or Algo orders, and matching
Hedge quantities. Any drift, unknown status, incomplete history or failed
query blocks the review.

`ReviewSignedAccountFramesV15` separately invokes v14 `AssessExposure`, keeping
all authorization fields false and `manual_review_required=true`. If the local
owner report contains multiple or opposite-side owner lots, the product
adapter **does not invent a protection GroupID for any other position**: it
returns a manual reconciliation classification instead. A matching sum cannot
prove fill attribution or safe OCO when manual and model positions share a
Binance Hedge side.

## Limitations and blockers

- The model owner lots are based on *local imported signed fills* and bounded
  historical REST reviews. They are not lifetime account history or per-entry
  protection group certificates. Multiple entries by the same model can be
  aggregated under one local owner lot.
- The full signed baseline collector is expensive and bounded by Binance API
  pagination/retention. Avoid calling this from the market tick loop.
- Exact canonical full-account evidence matching is intentionally strict;
  funding, other orders or margin changes make the observation fail closed.
- No private WebSocket continuity, durable authenticated ledger coverage,
  TP/SL true OCO, pooled-side fill attribution, financial COMMIT, or protected
  Demo E2E has been established.
- Source-level tests on the whole GitHub checkout, Windows runtime tests, and
  real Demo API responses are still required. Never infer authorization from
  a read-only review's success state.
