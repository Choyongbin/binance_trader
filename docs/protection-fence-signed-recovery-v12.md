# v12 — signed observation reconciliation for the durable protection mutation fence

## Scope

This adds `internal/protectionfence/recovery.go` and tests **on top of v11**.
`Store.InspectRecoveryOnce` is a read-only, strictly non-authorizing observer of
one unresolved account-wide `MAY_HAVE_SENT` record.

It performs the following sequence:

1. Integrity-check *all* fence files; require exactly one existing unresolved fence.
2. Ask a caller-supplied `SignedAlgoReader` for **two sequential authenticated Demo GET** observations of the recorded `clientAlgoID`.
3. Validate account fingerprint, group-bound target, quantity (strict string),
   position side, exchange Algo ID (especially for DELETE), freshness (5 seconds),
   state agreement and context cancellation.
4. Re-scan the durable fence to reject changes or corruption during the reads.
5. Return a diagnostic state, **never a trading, submission, cancellation, retry,
   settlement or fence-release authorization**.

States distinguish: observed active submit leg (NOT OCO), cancel target still
active, canceled target not financially settled, terminal/triggered requiring a
signed fill and full account audit, and absent/not-proven (NO retry). All
errors default to BLOCKED and every successful state remains manual review.

## Boundaries and explicit non-goals

- The `SignedAlgoReader` interface is **NOT** an authentication capability:
  callers must independently use real signed Binance Demo REST endpoints.
  These DTOs are not a digital signature, and even two identical GETs may be
  stale or cached. No HTTP endpoint, live transport, or control-plane path
  currently calls this interface.
- The existing `exchange.Client.QueryDemoAlgo` accepts a numerical `algoID`;
  first-submission ACK loss may lack that ID. A separate audited, signed,
  `clientAlgoID`-based lookup with complete bounded-history handling is needed
  before production integration.
- Bounded lookup absence is never proof that POST or DELETE was not sent.
  No automatic resubmission is ever permitted by this review.
- The single account-level fence **remains sealed** after every outcome;
  this deliberately blocks further mutations until an independently designed
  durable and safe multi-step reconciliation protocol is verified.
- Strict decimal string equality may intentionally flag otherwise numerically
  equivalent quantities for manual review, rather than risking rounding errors.
- Account-wide multi-model/pooled Hedge ownership, partial fills, simultaneous
  TP/SL trigger, unrelated manual orders, funding, fees and financial COMMIT
  remain unresolved. Two independent protective Algo orders are not OCO.
- The v11 file `Sync()` alone does not prove parent-directory persistence on
  every operating system. Full Windows crash/reboot testing is still required.
- `Arm`, `CancelTriggeredSibling`, HTTP START/STOP/manual order 423, and
  all real Binance POST/DELETE permissions remain unchanged/disabled.

## Offline tests

Unit tests cover two matching reads, START/STOP independence of the store,
restart no-replay, canceled sibling, no record, unknown/terminal states,
client/exchange identity mismatch, account mismatch, stale/future results,
quantity drift, status drift, damaged fence, signed GET failures, canceled
contexts and unchanged authorization flags. All tests use scripted in-memory
readers and make **zero external network or exchange calls**.

## Integration prerequisites

1. Use the exact GitHub `main` SHA `738d628543a587255e5d11219bf4adb1bd018553`.
2. Apply v10 cumulative, v11 additive, then v12 additive **in that order**
   (or use the single v12 cumulative patch instead; never both).
3. Run `git apply --check` on exact checkout and complete Go tests and build.
4. Before any product wiring: independently authenticate signed GET by
   `clientAlgoID`, design durable fence transitions that never lose uncertain
   send intent, prove account-wide ownership across restarts, and obtain
   separate operator certification.
5. Demo live signed REST/WS, physical Windows durability and supervised
   simulation are not tested in this offline phase.
