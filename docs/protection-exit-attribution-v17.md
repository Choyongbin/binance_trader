# v17 — TP/SL exit-fill attribution review (read-only)

Baseline: GitHub `main` commit `738d628543a587255e5d11219bf4adb1bd018553`. Apply **one** cumulative v17 patch; do not layer v1–v16 cumulative patches on top of it.

## Added components

- `internal/protectionfence/exit_attribution_v17.go`: pure review over individual v16 entry groups, signed TP/SL Algo legs, signed actual-order trades, and two recent consistent full-account recovery frames. Exact `big.Rat` BTC quantities and signed USD fees/PnL; globally unique entry/exit trade IDs, exchange actual-order IDs and TP/SL Algo IDs. Checks complete fill sums, unambiguous terminal statuses, total side exposure and fence identity. No network and no on-disk writes.
- `internal/trading/product_exit_attribution_v17.go`: internal helper `buildSignedExitLegsV17` maps existing immutable `ProtectionStoredFill` checksums and plan owner identity to **independently supplied** signed trade and TWO stable Algo observations; signed trades must exactly match every imported protected fill. Does not retrieve from Binance by itself and must NOT be fed UI/strategy inputs.
- Paired tests for normal single-owner limited observation, pooled entries, partial and double trigger, altered account snapshots, mismatched Algo/order/trade IDs, USDT exact fee/PnL values, input permutation, checksum corruption and inconsistent remote read-back.

## Outcome semantics

- `SINGLE_ENTRY_EXIT_BOUNDED_CONSISTENT_NOT_SETTLED`: one independently built entry group, complete one-leg exit with sibling confirmed terminal, post-exit side flat. **NOT** proof of continuous ownership, lifetime trade completeness, OCO, funding attribution or model-level net PnL; no posting is allowed.
- `POOLED_HEDGE_EXIT_UNATTRIBUTABLE`: multiple same-side entry groups, even if target actual order IDs and signed fills are exact. Binance pools Hedge positions; no model-specific attribution permitted.
- `PARTIAL_EXIT_HALT_NOT_SETTLED`: a non-complete exit means known remaining exposure and requires manual resolution.
- `DOUBLE_TP_SL_TRIGGER_HALT_NOT_SETTLED`: both TP/SL legs executed on a single entry group; no automatic attribution or re-issue.
- `BLOCKED_EXIT_ATTRIBUTION`: corrupt, missing, stale, changed, overfilled or otherwise inconsistent evidence.

All results keep `ManualReviewRequired=true`, all mutation/settlement approval flags `false`; no `POST`, `DELETE`, HTTP route, auto-trading engine hook or financial COMMIT introduced. v11 mutation fences and HTTP 423 lock remain intact. Existing `InspectSignedExitFills`, `ReviewRecovery`, and independent signed account-history reads still need an approved production orchestration path before this pure input can claim trusted provenance.

## Important exclusions

1. The pure review accepts Go values, not cryptographic signed inputs. SHA-256 identifies the reviewed *input*, not Binance authorization. The product adapter only matches independently obtained data; it does not authenticate the caller's Go slice or independently fetch complete account history.
2. There is **no** automated allocation for shared LONG/SHORT or repeated model entries. This is a conservative **detection and manual-review** milestone.
3. The observed realizedPnL and commission are diagnostic signed-fill totals, **not** financially committed model net PnL. Funding, non-USDT fees, historical gaps, unrelated trades and external closes remain blocked/out of scope.
4. The v17 test results are from the extracted real `internal/protectionfence` code and a **synthetic trading compatibility module**. A true full GitHub repository `go test ./...` plus Windows Demo E2E is still required before deploying.
