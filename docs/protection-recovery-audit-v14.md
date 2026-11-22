# v14: TP/SL 복구 감사 원장 및 Hedge 공동 소유권 분류

This is an **offline, non-authorizing** safety layer on top of v11–v13 `internal/protectionfence`.

## Implemented

- `AssessExposure(fence, snapshot, now)` compares the persisted target group and all provided BTCUSDT owner quantities to both Hedge position-side quantities using exact decimal rationals. A duplicate local group, unknown model ID, inconsistent LONG/SHORT sum, stale observation, invalid account fingerprint or absent target always yields `BLOCKED_UNVERIFIED_ACCOUNT_EXPOSURE`. Multiple **same-side** owners yield `POOLED_HEDGE_SIDE_MANUAL_RECONCILIATION` even when the aggregate is numerically equal to the observed account amount. A single matching owner only yields `SINGLE_OWNER_BOUNDED_MATCH_NOT_AUTHORIZED`.
- `Store.AppendRecoveryAudit` saves v12 signed-recovery **classification results** plus the supplied exposure evidence in a sibling directory `trading/signed_recovery_audit_v14`, with O_EXCL numbered append-only files, a prior-record checksum link, and strict revalidation on read. It never clears/deletes the original account-wide `MAY_HAVE_SENT` mutation fence, creates POST/DELETE intents, or calls Binance.
- `Store.ReadRecoveryAudit` scans the complete local audit chain, rejects unknown or orphan files, missing sequence numbers, symlinks, corrupted bytes, reordered timestamps, changed permission fields and mismatched fence checksums. It does not create a directory when one is absent.
- All review permission booleans stay false, and all results require manual review. Concurrent attempts to append the same sequence can have only one O_EXCL winner; an incomplete concurrent write blocks subsequent reads instead of being silently skipped.

## Security limitations (important)

- `ExposureSnapshot` and `RecoveryReview` are **caller-supplied local structures**. They are not cryptographic Binance signatures. The new code does not independently query the account or attest entire exchange history, unobserved external trades, streaming continuity, other symbols, margin collateral, fees, funding, or owner attribution. Never allow this assessment to authorize a real mutation or financial COMMIT.
- SHA256 checksum chains detect many accidental corruptions but are not keyed signatures against an adversary who can rewrite local files and recompute hashes.
- File `Sync()` and O_EXCL do not prove Windows directory-metadata survival across power loss. A process holding the existing `Coordinator` lock must own audit reads and writes in any future product integration.
- **Not yet wired** to `DemoProtectionRuntimeCoordinator`, the HTTP endpoints, scheduled watcher, private Binance API, or trading loop. Existing `Arm`, `CancelTriggeredSibling` and HTTP 423 safeguards remain unchanged.
- The journal is stored **beside**, not *inside*, `product_mutation_fences`, because the existing v11 `InspectAll` intentionally rejects all unknown files/subdirectories in the fence slot.

## Validation scope

The isolated Go package test includes the exact v11–v13 `protectionfence` source and tests extracted from the user-provided cumulative v13 patch, plus the new v14 files; no exchange or trading handwritten stubs are needed for this package. Full GitHub `738d628` integration, real Binance Demo and end-to-end Windows integration remain outstanding.
