# v16 — Per-entry TP/SL lineage under pooled Hedge positions

Scope: `738d628` baseline + v1–v15 cumulative patch + v16 additive patch.

## Implemented

- `internal/protectionfence/entry_lineage_v16.go` is a pure, read-only, strictly non-authorizing reviewer. Each *individual entry order* (`entry_<clientOrderID>`), even when multiple orders share a `modelID`, is joined to independently identified trade IDs, the full exchange order ID, original and executed quantities, immutable protection plan hash, and distinct TP/SL client Algo IDs. Duplicates, partial fills, missing plan/trades, mismatched quantities, and observed exits cause a blocked/manual review state.
- `internal/trading/product_entry_lineage_v16.go` implements `InspectProductEntryGroupLineageV16(ctx, fromMS)` as an operator-only product method, **NOT attached to an HTTP route, auto engine, or exchange mutation**. It reads the existing durable local request/fill/protection journals, validates existing mutation fence and coordinator lease, performs two independent Demo signed REST full-account baselines, uses existing read-only `ReviewRecovery` and v15 account frame normalization, reconstructs entries against actual signed trade/order IDs, and checks that the local journal/fence has not changed.
- Per-entry planned TP/SL *identity* and quantities are compared. This is **not** proof TP/SL currently exists, that either leg is active, that Binance guarantees atomic OCO, or that an exit fill can be attributed to a particular entry.
- Same model opening 2 positions generates 2 distinct entry groups; same-side different models similarly produce `MULTI_ENTRY_POOLED_HEDGE_UNATTRIBUTABLE`. No virtual per-model group is invented from a pooled balance.
- Signed account windows are bounded and their contents are not stable between snapshots in general. Unknown outside-history, user-stream epoch gaps, funding/fees and external trading always require other review. Every state sets `manual_review_required=true`, and trade/submit/cancel/retry/settlement authorization remains false. A matching GET can never clear a mutation fence.

## Explicit limitations and safety locks

- The test types are *not* signed; only the trusted production product adapter obtains signed GETs. The reviewer cannot be consumed as an executable authorization token.
- The current product bridge intentionally refuses histories that omit old entry trades, multiple unlinked/settled plan groups, and unknown or missing signed events. No attempt to silently synthesize group quantity from aggregate `OwnershipReport.Owned` is allowed.
- A protective exit or earlier order close is not allocated to one specific entry group; the historical entry totals cannot be treated as open quantities after exits. A separate signed fill-to-owner allocator would be required and must account for pooled same-side exchange position.
- Live `Arm` and `CancelTriggeredSibling` remain disabled (`ErrProtectionApproval`); automatic trading HTTP remains 423. No POST/DELETE or account mutation was called.

## Test scope

- Go `internal/protectionfence` v11–v16 implementation tests on locally extracted patch sources, including race detection.
- New adapter helper tests with compiler-compatible synthesized dependencies, **not a full build of the actual GitHub repo**. The source method's interfaces/types were checked against the GitHub `738d628` source, but the actual exact full checkout was unavailable in the testing container.
- Before enabling any trading, a verified full Git checkout, cumulative `git apply --check`, `go test ./...`, `go vet ./...`, Windows build and prolonged Binance Demo E2E are mandatory. Do not remove HTTP 423 based solely on this work.
