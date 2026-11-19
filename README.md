## v0.4.11-dev.3.138 — 장시간 모의 다중 모델 위험·장애 주입 감사 (HTTP 423 LOCKED)

- `AuditOfflineMultiModelSoak`는 3.137의 signed Equity 위험 미리보기와 영속 RUN/STOP 세대를 **오프라인 다중 캔들 추적**으로 반복 평가. 실제 체결/주문 실행 없이 후보·중복 억제·증거 실패·Equity 차단을 분리 집계.
- 테스트용 가상 시계로 24시간 1,440개의 1분봉과 96개 중복 이벤트를 재생. WS 수신 지연 및 signed 계정 증거 불일치에서 차단, 같은 분 내 증거 복구 시에만 재검토; 1분봉 유실/역행은 추적 전체에 `GAP_REQUIRES_RECONCILIATION`으로 고정하고 자동 연결 복구 후에도 후보 생성 금지.
- STOP은 신규 진입 후보만 차단하고 소유 포지션은 그대로 둠. 프로세스 재시작으로 이전 RUN ID를 소유하지 않는 경우에도 `RESTART_BLOCKED` 유지. 손상된 제어 파일 및 사전 취소 Context 차단.
- `scripts/pc-demo-multi-soak-offline-tests.ps1`, `docs/DEV3138_OFFLINE_MULTIMODEL_SOAK_KO.md` 추가. 이는 **가상 시간의 시뮬레이션**이며 실제 24시간 실시간 E2E, Binance private WS, 모델 성능·체결·확정 PnL 검증이 아님. 제품 START/STOP/주문 HTTP 423 유지.

### v0.4.11-dev.3.137 — 다중 모델 공유 위험·RUN/STOP 세대 오프라인 감사 (HTTP 423 LOCKED)

- `internal/trading/multi_model_risk_review.go`: 기존 `ReadOnlyMultiModelScheduler`의 신호 평가와 signed account USDT Equity/자동 모델 전체 1~30% 초기 증거금 한도, 영속 RUN/STOP 세대를 읽기 전용으로 교차 검증합니다. STOP·비정상 재시작에서는 신규 진입 신호를 평가하지 않으며 기존 포지션·보호주문을 그대로 둡니다.
- 테스트: `go test ./internal/trading -run '^TestOfflineMultiModelRiskReview' -count=1` 또는 `scripts/pc-demo-multi-risk-offline-tests.ps1`. 실제 Binance 인증·네트워크·주문 없이 모의 증거로 검사합니다.
- **진입 예산은 원래의 공유 배정 지갑 기준 10%이며, 새 30% Equity 검사는 오프라인 모의 위험 감사에 한정**됩니다. 현재 제품 서버가 이 감사를 사용해 주문을 실행하는 것이 아닙니다. 실제 거래 연결 전 독립 signed 재검증 및 위험 게이트 통합 필요. 등록 모델은 2개, 다중 실행은 첫 주문 시도 후 중단. HTTP 423 잠금 유지. `docs/DEV3137_OFFLINE_MULTIMODEL_RISK_STOP_KO.md` 참고.

### v0.4.11-dev.3.136 — 실제 모델 레지스트리·다중 모델 설정 감사 (HTTP 423 LOCKED)

`model.DefaultRegistry`의 내장 전략 **2개**와 UI 모델 catalog 및 선택 설정을 대조하는 읽기 전용 모델 감사 기능. 두 모델은 과거의 Logistic 5개가 아닙니다. `GET /api/v1/models/runtime`에 감사 결과를 노출하고 미등록/중복/불일치 선택을 차단, 새 오프라인 `cmd/model-registry-audit`와 PowerShell 스크립트로 데이터 쓰기 없이 확인할 수 있습니다. 다중 모델 실행기는 공유 계정·예산·Hedge-side 및 최초 주문 후 감독형 종료 계약을 유지합니다. 거래·확정 PnL·금융 COMMIT 권한은 없고 **HTTP 423 LOCKED**를 유지합니다. 자세한 내용: `docs/DEV3136_MODEL_REGISTRY_AUDIT_KO.md`.

## v0.4.11-dev.3.135 — OFFLINE STOP/restart protection incidents (LOCKED)

- `INSPECT_ONLY` is now genuinely read-only when no entry protection plan exists; it does not recover fills or persist a plan. Explicit simulator-only `ARM` remains a separate operation.
- STOP invalidates model-entry run lease but never force-closes a preexisting position; read-only signed protection review, simulated operator-only sibling cancel, partial/double triggers and post-DELETE ACK loss are covered by deterministic regression tests.
- Production HTTP 423 LOCKED, real exchange POST/DELETE and financial COMMIT are unchanged. See `docs/DEV3135_OFFLINE_STOP_PROTECTION_INCIDENTS_KO.md`.

## v0.4.11-dev.3.134 — OFFLINE entry → TP/SL → exit lifecycle E2E (LOCKED)

- Signed-only entry `MAY_HAVE_SENT` recovery and deterministic TP/SL plan are wired to a **sealed mock-only** protection arming/sibling-cancel simulator. Restart never replays ambiguous POST/DELETE; partially filled or double-triggered orders HALT.
- This is NOT production Binance trading integration: the product HTTP 423 lock, actual financial COMMIT, auto TP/SL emission and confirmed model PnL stay disabled. Full scope and test limitations: `docs/DEV3134_ENTRY_TO_PROTECTION_OFFLINE_E2E_KO.md`.

> **dev.3.133 (HTTP 423 LOCKED):** Read-only cross-incident model cost grouping with exact USDT fee/realized-PnL invariants. Account funding is ALWAYS unallocated (`null` model funding), even when the observed amount is zero. No settlement, account attestation or trading authority. `docs/DEV3133_OFFLINE_MODEL_COST_POLICY_KO.md`.

> **dev.3.132 (HTTP 423 LOCKED):** Adds pinned read-only recovery and one-stage simulated restart for the isolated 3.131 mock multi-ledger COMMIT, with deterministic interrupted/ambiguous-ACK/torn-slot tests. This is **not** real financial posting or a Binance account continuity proof. See `docs/DEV3132_MOCK_COMMIT_RESTART_RECOVERY_KO.md`.

### dev.3.131 오프라인 다중 원장 단일 커밋 시뮬레이터

이미 봉인·전역 예약된 2~64개 거래 사건에 대해 별도 모의 저장소의 Lifecycle·Ownership·Financial 뷰를 마지막 COMMIT 전까지 숨기고, 완료 후 세 뷰를 함께 제시합니다. **실원장 반영이나 Binance 주문이 아닙니다.** `scripts/pc-demo-mock-financial-commit.ps1`, `docs/DEV3131_MOCK_MULTI_LEDGER_ATOMIC_COMMIT_KO.md`를 참고하세요. HTTP 423 잠금 유지.

## v0.4.11-dev.3.130 — 오프라인 독립 서명 검토 증거·nonce 재사용 차단 (HTTP 423 LOCKED)

- `ExternalIndependentApprovalRequest`: 3.125 전역 예약·실험적 원자적 봉인/금융 preflight을 사건별 정확한 해시에 고정한 **비승인 검토 요청**. 32바이트 nonce·유효기간(최대 24시간)·서명 대상 유일성을 검증합니다.
- `ExternalIndependentReviewAttestation`: 독립적으로 고정한 Ed25519 공개키 SHA-256과 분리된 서명 바이트 도메인을 확인합니다. 요청·모델·봉인·기간·결정·서명 위조를 거부합니다.
- `ExternalIndependentReviewReceiptStore`: 독립 기존 디렉터리에 `O_EXCL`+`fsync`로 일회성 nonce 사용을 기록합니다. 재시작/동시 사용 차단; 중단된 기록은 수동 검토가 필요하며 원본 원장은 수정하지 않습니다.
- CLI/PowerShell: `cmd/demo-close-independent-review`의 `intent|verify-once`, `scripts/pc-demo-independent-review.ps1`. 외부 서명 private key를 보관하거나 생성하지 않습니다.
- **독립 서명 검증은 실제 금융 승인이나 계정 전체 signed 증거가 아닙니다.** 확정 손익·원자적 실원장 COMMIT·자동매매 활성화 없이 HTTP 423 LOCKED 유지. 자세한 내용 `docs/DEV3130_INDEPENDENT_OFFLINE_REVIEW_ACK_KO.md`.

### v0.4.11-dev.3.129 — 오프라인 기준선 검증·정산 승인 차단

3.128 체크포인트와 3.127 제한 구간 연속성 증거를 3.126 모델별 수수료·펀딩 감사와 결합해, 수집된 **진입~청산 구간**만 일치하는지 검증합니다. 계정 전체 이력·다른 종목·private stream·펀딩 귀속·실정산 승인은 전혀 입증하지 않으며 권한은 항상 false입니다. `docs/DEV3129_OFFLINE_HISTORY_BASELINE_GATE_KO.md` 참고. 제품 HTTP 423 LOCKED 유지.

### v0.4.11-dev.3.128 — 오프라인 signed GET 체크포인트·재시작 검증

오프라인 모의 응답을 불변 페이지 체크포인트로 저장하고 실패·재시작·중복 페이지 한도를 검증합니다. 최종 봉인은 3.127 bounded 이력 감사와 대조하며, 실제 signed GET/거래/정산/계정 전체 증명은 아닙니다. 제품 HTTP 423 LOCKED 유지. `docs/DEV3128_OFFLINE_HISTORY_CAPTURE_CHECKPOINT_KO.md` 참고.

### v0.4.11-dev.3.127 — 오프라인 signed 이력 구간 연속성 감사

기존 3.126 정산 증거에 더하여, 보관된 `BTCUSDT` signed GET 거래/펀딩 응답의 JSON window manifest가 **해당 사건의 진입~청산 구간에서 연속되는지** 대조합니다. 구간 누락·겹침·잘림 의심 및 원본 거래 불일치를 차단하지만 **계정 전체 히스토리 증명이 아닙니다.** 실제 자동매매/금융 COMMIT은 계속 HTTP 423 LOCKED입니다.

`go run ./cmd/demo-close-financial-review -model-evidence -history-windows <windows.json> -data <offline-snapshot> -id <clientOrderID> -incident <package-a-incident.json>`

실행 계약·입력 JSON 형태: `docs/DEV3127_SIGNED_HISTORY_CONTINUITY_KO.md`.

> **dev.3.126 (HTTP 423 LOCKED):** Read-only per-fill USDT fee / realized PnL evidence contract; explicit unallocated funding transaction trace with null model funding and null final model net. No settlement authority. See `docs/DEV3126_MODEL_FEE_FUNDING_EVIDENCE_CONTRACT_KO.md`.

> **dev.3.125 (HTTP 423 LOCKED):** Adds a STRICTLY READ-ONLY cross-check between a previously reserved 3.124 full incident batch and each event's sealed, non-posted atomic ledger rehearsal. A partially sealed batch has no jointly visible candidates. This is NOT a real financial commit or Binance account history proof. See `docs/DEV3125_OFFLINE_GLOBAL_ATOMIC_CONSISTENCY_KO.md`.

> **dev.3.124 (HTTP 423 LOCKED):** Adds a separate OFFLINE, NON-POSTED append-only cross-incident evidence reservation registry with exclusive immutable slots, checksum chain, collision detection across batches, restart and crash fail-closed simulation. It is **not** a real financial reservation/commit or shared-account proof. See `docs/DEV3124_OFFLINE_GLOBAL_EVIDENCE_RESERVATION_KO.md`.

> **dev.3.120 (HTTP 423 LOCKED):** New GET-only, two-pass authenticated full-account signed REST inspection gates prepared TP/SL against actual immutable reservation, FILLS_IMPORTED lifecycle, journal/fill ownership, unmatched trades/orders, unrelated exposures, stale/changed baselines and local evidence drift. Results are **review-only**, never order approval. No Binance POST/DELETE, no live runtime unlock. See `docs/DEV3120_PROTECTION_EXECUTION_APPROVAL_GATE_KO.md`.

> **dev.3.119 (HTTP 423 LOCKED):** Adds an OFFLINE-only sealed TP/SL POST → signed recovery → one-shot sibling DELETE rehearsal with crash/restart and ACK-loss tests. Fixes signed GET-only review of a plan-matched open Algo after an ambiguous POST. The real Demo runtime `Arm` / `CancelTriggeredSibling` convenience methods now reject with `ErrProtectionApproval`; **no real Binance POST/DELETE is enabled**. See `docs/DEV3119_OFFLINE_PROTECTION_DISPATCH_KO.md`.

> **dev.3.118 (LOCKED):** Adds a signed GET-only continuation of the 3.117 recovered-entry TP/SL plan: durable arming checkpoints, independent Algo identities and actual exit trades, two account preflights, single-trigger/sibling cancel intent audit, crash/restart and fail-closed classifications. No Binance POST/DELETE, financial settlement or product runtime unlock. See `docs/DEV3118_PROTECTION_LIFECYCLE_READONLY_KO.md`.

> **dev.3.117 (LOCKED):** Adds a supervised signed-entry-fill recovery → full ownership audit → deterministic TP/SL local preparation bridge with NO exchange submissions, plus mock failure/restart coverage. Existing TP/SL arming is separately supervised and is **not** invoked. No unattended auto runtime or trading HTTP route is unlocked. See `docs/DEV3117_RECOVERED_ENTRY_PROTECTION_BRIDGE_KO.md`.

> **dev.3.116 (LOCKED):** Fixes Codex audit R-07/R-08/R-09: require externally verified package SHA-256 before executing extracted PowerShell, retire no-TP/SL `pc-tonight-demo.ps1 -DemoOneOrder` path, and align bootstrap phase number with version source. See `docs/DEV3116_DEPLOYMENT_AUDIT_FIX_KO.md`. Real financial COMMIT, product TP/SL orchestration and unmanned Demo trading remain unavailable.

> **dev.3.115 (HTTP 423 LOCKED):** Common, origin-explicit REAL vs SHADOW read-only port with strictly separate SHA-256 pins and an opt-in offline `source` CLI. No financial posting or live trade authorization. See `docs/DEV3115_COMMON_LEDGER_READ_PORT_KO.md`.

> **dev.3.114 (HTTP 423 LOCKED):** Adds an opt-in, caller-pinned REAL ownership/lifecycle combined offline audit (`demo-ledger-consistency -action bundle`). The local evidence hash is NOT a financial COMMIT and does not authorize trades or settlement. See `docs/DEV3114_PINNED_REAL_LEDGER_KO.md`.

> **dev.3.113 (HTTP 423 LOCKED):** Read-only actual ledger observation and token-pinned SHADOW comparison. This does not replace existing live readers or authorize financial posting. See `docs/DEV3113_REAL_SHADOW_LEDGER_BOUNDARY_KO.md`.

> **dev.3.112 (HTTP 423 LOCKED):** Adds an isolated shadow two-ledger prepare/publish/read protocol with crash-restart and integrity checks. No real financial COMMIT, no Binance order, no trade authorization. See `docs/DEV3112_SHADOW_COMMIT_PROTOCOL_KO.md`.

> **dev.3.111 (HTTP 423 LOCKED):** Adds event-scoped, read-only financial COMMIT preflight and repairs offline PC scripts pinned to older releases. Every valid result is **BLOCKED**, not permission to post a financial ledger. See `docs/DEV3111_FINANCIAL_COMMIT_PREFLIGHT_KO.md`.

> **dev.3.110 (HTTP 423 LOCKED):** Opt-in event-scoped settlement journal + atomic non-posting rehearsal consistency inspection (`-LinkedEvent`), with a pinned SHA-256 for both Lifecycle and Ownership candidates. **No financial posting or automatic orders**; existing legacy sidecars are not migrated. See `docs/DEV3110_EVENT_CONTINUITY_KO.md`.

> **dev.3.109 (HTTP 423 LOCKED):** Added opt-in per-clientOrderID *NON-POSTED* settlement journal sidecars, with read-only legacy inspection and cross-process collision tests. Does **not** apply financial settlement or activate orders. The upper settlement evidence chain still has single-event constraints. See `docs/DEV3109_SETTLEMENT_JOURNAL_EVENT_SCOPING_KO.md`.

> **dev.3.108 (HTTP 423 LOCKED)**: Added per-clientOrderID NON-POSTED atomic rehearsal event directories and explicitly read-only legacy inspection. These are *not* real financial posting or trade authorization. See `docs/DEV3108_ATOMIC_EVENT_SCOPING_KO.md`.

> **3.107 개발 체크포인트:** 3.106의 미적용 정산 리허설에 `commit_sha256`을 고정하는 공통 이중 장부 **후보 조회**를 추가했습니다. 실제 Lifecycle/Ownership/PnL 원장에 적용하지 않으며, 제품 주문 HTTP 423 잠금은 유지됩니다. `docs/DEV3107_ATOMIC_PAIR_READ_KO.md`를 참고하세요.

> **3.106 개발 체크포인트 (2026-11-04):** 외부 청산 이중 장부의 원자적 가시성을 검증하는 **미적용 리허설**을 추가했습니다. 금융 정산/실거래는 활성화되지 않았으며 START·STOP·수동 주문은 HTTP 423 잠금입니다. 자세한 단계·제약은 `docs/DEV3106_ATOMIC_REHEARSAL_KO.md`를 확인하세요.

> **3.104 safety checkpoint:** `InspectSignedExitFills` independently cross-checks signed TP/SL actual-order fills and detects oversold/double-trigger/partial exits, but does not post any financial ledger, establish pooled-side ownership/funding, or authorize trading. Product HTTP 423 remains LOCKED.

**개발 체크포인트: v0.4.11-dev.3.105 (HTTP 423 LOCKED).** signed 종료 체결과 전체 Hedge 방향 소유권 노출을 읽기 전용으로 대조하는 점검기를 추가. 실제 서버 연결·정산·자동매매는 미완료입니다.

> **3.102 development checkpoint (LOCKED):** Added local-only, non-authorizing sibling cancellation identity/restart continuity inspection and rejection of duplicate TP/SL exchange Algo IDs. No automatic cancel/replay, settlement, or product order path enabled.

**개발 체크포인트: v0.4.11-dev.3.101 (HTTP 423 LOCKED).** 내부 감독형 진입 체결→TP/SL 호출 경로를 모의 거래소로 검증했습니다. 제품 자동매매 연결 및 전체 정산은 미완료입니다.

## v0.4.11-dev.3.100 — 진입·보호주문 증거 연결 검증 강화 (LOCKED)

- 불변 예약의 원본 Intent와 Lifecycle Root 실행 계획·Digest를 직접 대조합니다.
- 최종 `FILLS_IMPORTED` 실행 수량과 예약·소유 수량을 정확한 십진수로 비교합니다.
- LONG/SHORT 변조·재해시·버전 테스트를 추가했습니다. 기존 제품 주문 HTTP 423 잠금은 그대로이며, 실제 TP/SL 자동 전송·외부 청산 원자적 정산은 미완료입니다.

## v0.4.11-dev.3.99 — 진입 체결·보호주문 상태 통합 감사 (LOCKED)

- `InspectEntryProtectionContinuityGroup`는 서명 조회 결과, 진입 Lifecycle 해시 체인, 소유권·수량, TP/SL 영속 체크포인트를 **읽기 전용**으로 함께 검증합니다.
- 모의 거래소에서 정상 TP/SL 발행, SL ACK 유실, 재시작 중복 제출 방지, 서명 데이터 누락·변조를 검사했습니다.
- **실제 자동주문 및 TP/SL 실행 연결이 아닙니다.** HTTP 423 잠금 유지. 외부 청산 원자적 정산 미완료. 운영자 승인 없이 ZIP을 실환경에 적용하거나 거래를 활성화하지 마세요.

## v0.4.11-dev.3.98 — 진입→보호주문 인계 Lifecycle 검증 강화 (LOCKED)

진입 체결 후 보호주문으로 인계하기 전 전체 Lifecycle 단계의 순서·시각·해시 체인을 읽기 전용으로 검증합니다. 중간 이벤트 누락·변조·재서명 등은 차단됩니다. 실제 자동 TP/SL 실행·청산 정산·다중 모델 연속 주문은 아직 연결/완료되지 않았으며 HTTP 423 거래 잠금을 유지합니다.

## v0.4.11-dev.3.97 — 보호주문 체크포인트 재시작 검사 (LOCKED)

- 보호주문 로컬 체크포인트를 읽기 전용으로 분류: 준비, TP 결과 불명, TP만 이전에 확인, SL 결과 불명, TP/SL 모두 이전에 확인.
- 두 보호주문 레그가 동일 거래소 Algo ID로 관측되면 fail-closed. 재시작 및 손상된 영속 파일 회귀 검사 추가.
- 단순 로컬 과거 확인은 현재 보호주문 활성 상태의 증거가 아닙니다. 상세: `docs/DEV397_PROTECTION_CHECKPOINT_KO.md`.
- 자동 TP/SL 전송, 원자적 정산, Binance Demo E2E 미완료. HTTP 423 유지.

# v0.4.11-dev.3.96 — TP/SL 서명 preflight 안전성 강화 (HTTP 423 LOCKED)

- `validateProtectionPreflight`가 Demo signed bracket의 시작/종료 시각과 최대 5초 수행 시간, nil 미체결 일반/Algo 주문 목록을 **차단**합니다. 빈 배열(`[]`)과 응답 누락(`null`)을 구분합니다.
- `exchange.ValidatePortfolioSnapshot`으로 USDT 자산, 전체 포지션과 숫자 필드의 완전성을 확인합니다. 보호주문 수량 일치 검사 전에 계정 응답이 불완전하면 제출하지 않습니다.
- TP `MAY_HAVE_SENT` 영속화 이후 HTTP POST 전에 context 취소 여부를 다시 확인합니다. 이 경우에도 기록을 지우거나 무조건 재전송하지 않습니다.
- LONG/SHORT 보호주문 기존 실패 주입 테스트를 유지하고, 누락 응답·잘못된 시각·중복 계정 포지션·취소 시 POST 0건 테스트를 추가했습니다.

**범위 제한:** 이번 릴리스는 기존 내부 보호주문 경로의 *사전검증 강화*입니다. 진입 ACK 유실부터 TP/SL 자동 제출까지 연결하는 제품 서버 통합, 실제 형제 주문 자동 취소, 사용자 사고의 정산, 다중 모델 주문 활성화는 아직 미완료입니다. 어떤 테스트도 실제 Binance 주문을 보내지 않으며 모든 제품 주문 API는 HTTP 423을 유지합니다.

---

# v0.4.11-dev.3.95 — read-only entry → protection handoff review (HTTP 423 LOCKED)

`ReviewEntryProtectionHandoff` cross-checks a single immutable entry reservation and `FILLS_IMPORTED` lifecycle event with an owner-specific pre-staged TP/SL pair and a fresh signed Demo account preflight. Incomplete or stale evidence, different quantities, wrong side, tampered checksums and unexpected open Algo orders are blocked.

**This is a pure, shape-only, non-authorizing review.** It does not verify the entire local hash chain or independent exchange provenance itself; callers must load verified records from their stores. It never submits/cancels an order, persists a journal or authorizes trading. Crucially, Binance's two conditional orders are not atomic OCO. The live product order path is still HTTP 423 LOCKED. See `docs/DEV395_ENTRY_PROTECTION_HANDOFF_KO.md`.

**Still missing:** live entry-to-protection orchestration, full lifecycle/account-wide reconciliations, automated sibling cancel/readback, external close atomic posting, real multi-model order activation and actual PC Demo E2E. Do not treat this checkpoint as ready for unattended trading.

---

# v0.4.11-dev.3.94 — offline settlement evidence matrix (HTTP 423 LOCKED)

`ExternalSettlementJournalStore.InspectEvidenceMatrix` rechecks already-sealed local proposal, posting gate, entry/exit trade IDs and restart checkpoint integrity without contacting Binance or writing any journal. It reports evidence checks and remaining blockers, not permission to settle.

In an existing offline PC installation with the required reviewed sidecars, run `scripts/pc-demo-close-settlement-journal.ps1 -ClientOrderID bt_... -EntryArchiveFile package-a-entry-archive-...json -Action inspect-evidence`. This read-only operation fails closed if prerequisites are missing or altered. See `docs/DEV394_SETTLEMENT_EVIDENCE_MATRIX_KO.md`.

**Not implemented:** financial posting, complete account history attestation, model funding allocation, external close ownership determination, trading enablement, automatic TP/SL. This release deliberately retains HTTP 423. No Demo order or user data changed.

---

# v0.4.11-dev.3.93 — 오프라인 정산 저널 재시작 검사 (주문 LOCKED)

상세: `docs/DEV393_SETTLEMENT_RESTART_INSPECTION_KO.md`. 이 단계는 정산 적용이 아닌 검증 단계입니다. 원본 장부, 주문 API, Demo 계정은 변경하지 않습니다.

---

## Current development checkpoint: v0.4.11-dev.3.92 (READ-ONLY PRODUCT / HTTP 423 LOCKED)

This cumulative source builds on dev.3.91. An **internal-only**, explicitly scoped Go recovery method (`ExecutionLifecycleStore.RecoverOwnedEntryFillsOnly`) can use authenticated Demo signed reads to idempotently import an existing model/manual **entry** fill and persist lifecycle evidence through `FILLS_IMPORTED`. It never submits an order, converts an unrelated external close into an owned close, consumes the original reservation, finalizes financial settlement or permits trading. There is intentionally **no CLI/HTTP/UI route** for this local mutation until external-account safety evidence and operator approval requirements are completed. No user data or Binance account was touched by the development tests.

Original external-close reconciliation remains blocked by unresolved account-wide history, per-model funding attribution, approved atomic cross-ledger posting and explicit operator verification. This version does not resolve the existing incident automatically; `MAY_HAVE_SENT` will remain unchanged on the user's PC until a separately authorized recovery actually runs. See `docs/DEV392_ENTRY_ONLY_RECOVERY_KO.md`.

---

# Latest cumulative development checkpoint: v0.4.11-dev.3.91 (LOCKED)

This cumulative source release builds on dev.3.90. It wires the already-implemented shared multi-model runtime constructor into the product server's **internal factory only**, while HTTP START/STOP/orders remain **423 LOCKED**. This is NOT live multi-model execution or a trading-approved release. See `docs/DEV391_MULTI_MODEL_RUNTIME_WIRING_KO.md`. All past safety and settlement blockers remain in effect.

---

# Latest cumulative development checkpoint: v0.4.11-dev.3.90 (LOCKED)

This is a full cumulative source release, upgrading dev.3.89. It adds an explicit **signed GET-only** `recheck-entry-recovery` action and safely escapes dynamic operational banner text. It is **not** an automatic missing-fill importer or financial posting implementation. The historical SHORT 0.001 BTC gap remains unresolved until separate authorized recovery and reconciliation.

On a Windows PC, with the existing server stopped and the original `data/`, `pc-results/` and `.enc` retained, an operator can optionally run the following **only after installation**:

```powershell
cd F:\binance_trader
.\scripts\pc-demo-package-a.ps1 -Action recheck-entry-recovery -ClientOrderID package-a-entry-archive-20261029_001413_310-32e13249.json
```

The argument is an **existing saved entry-archive basename**, not a clientOrderID in this action. This fetches fresh signed Demo order and trades (GET only), checks the current local immutable originals, and emits a new `pc-results/package-a-signed-...json` report. It never changes the original data/ledger, imports fills, settles ownership, posts an order or authorizes trading. Network data may be unavailable due to Binance API retention limits; failures remain fail-closed. Do not attach `.enc` or exchange credentials to chat. Original snapshots are local SHA-256 checked copies, not Binance-signed archives.

Implementation and limitations: `docs/DEV390_SAFE_ENTRY_RECHECK_KO.md`.

---

# v0.4.11-dev.3.89 — 과거 signed 증거의 빈 펀딩 해시 호환성 수정 (LOCKED)

3.88 실제 Demo 수집 후 `signed GET detail fingerprint does not match archived contents`가 발생한 문제를 Go JSON round-trip 재현으로 확인하고, 펀딩 0건(`[]` ↔ nil)인 경우 원본 signed 상세 지문을 검증할 수 있도록 수정했습니다. 새 복구 명령은 기존 `scripts/pc-demo-package-a.ps1 -Action diagnose-discovery-archive`이며, 원본 파일은 수정하지 않습니다. 상세 `docs/DEV389_ARCHIVED_ZERO_FUNDING_FIX_KO.md`. 주문·정산·원본 장부 수정 권한은 계속 없음(`HTTP 423 LOCKED`).

---

# v0.4.11-dev.3.88 — PC 검증 전 안정화 (HTTP 423 LOCKED)

기존 dev.3.87 전체 누적 소스를 유지하며 **계정 미실현 PnL 숫자 오류**를 수정하고, STOP·finalCheck·HTTP POST 경계의 모의 실패 주입 테스트를 보강했습니다. 계정 snapshot의 포지션·주문 목록이 불완전하면 0건/0 USDT로 추정하지 않고 미확인으로 표시합니다. 다중 모델 생성자의 nil Registry도 안전하게 거부합니다. 주문 직전 취소된 context는 제출 전에 중단합니다.

**정확한 보장 범위:** 이 코드는 읽기 전용 PC 진단 준비 릴리스입니다. 실제 Demo 자동매매·수동 주문·보호주문 자동 복구·확정 손익 posting은 아직 제공하지 않습니다. STOP보다 먼저 시작한 네트워크 POST는 되돌릴 수 없으며, signed 주문·체결 재조사가 필요합니다. 모든 주문과 자동 START/STOP은 **HTTP 423 LOCKED** 유지. Demo의 설정 변경 `apply-demo-v1`은 별도의 실제 변경 API이므로 초기 PC 읽기 전용 테스트에서 호출하지 않습니다.

검증·수정 범위: `docs/DEV388_PRE_PC_SAFETY_FIXES_KO.md`, `docs/DEV388_TEST_REPORT_KO.md`.

---

# v0.4.11-dev.3.87 — 5회차: 모델별 한글 읽기 전용 대시보드 (HTTP 423 LOCKED)

기존 dev.3.86 누적 코드 및 3.83~3.85 증거/보호/정산 차단 구조를 유지하면서 웹 UI의 모델별 현황을 GET `/api/v1/trading/model-dashboard`에 연결했습니다. 모델별 LONG/SHORT는 로컬 Ownership 증거만 표시하며, 모델별 확정 실현·미실현 손익은 독립 signed 정산/귀속 검증이 없어 `— (미검증)`으로 유지합니다. START/STOP 및 모든 주문 엔드포인트는 계속 HTTP 423 잠금입니다. 실제 Binance Demo PC/실주문 검증은 미실시입니다.

상세: `docs/DEV387_READONLY_MODEL_DASHBOARD_KO.md`, `docs/DEV387_TEST_REPORT_KO.md`.

---

# v0.4.11-dev.3.86 — 패키지 C 다중 모델 안전 실행기 (내부 전용, HTTP 423 LOCKED)

기존 3.85 누적 코드를 바탕으로 frozen Model Registry를 하나의 공유 Coordinator/Ownership/지갑/Risk/Lifecycle/Executor 경로로 연결한 **내부용 감독 실행기**를 추가했습니다. 선정된 모델을 직렬 평가하며 한 번의 주문 시도 또는 오류 이후 후속 모델 실행을 중단합니다. 주문 ACK 후 signed 검증을 요구하고 동일 AutoController의 START 재시작도 금지합니다. 이는 병렬 주문이나 모델별 실제 금융 격리, 보호주문 OCO, 완전 자동매매 가동이 아닙니다.

**서버 자동 START·수동 주문은 계속 HTTP 423 LOCKED**이며, 새 멀티모델 실행기는 웹 라우트에 연결되지 않았습니다. 실제 Binance Demo 주문과 Windows PC는 미검증입니다. 자세한 구현 및 제한: `docs/DEV386_MULTIMODEL_EXECUTION_KO.md`, 검증 결과: `docs/DEV386_TEST_REPORT_KO.md`.

---

## v0.4.11-dev.3.85 — 패키지 B 3회차: TP/SL 회복 상태 머신과 pooled-side 안전 판정 (READ ONLY)

기존 dev.3.84 전체 소스를 유지하고, `internal/trading/protection_recovery_review.go`의 순수 계정 전체 복구 검토기를 추가했습니다. 진입/청산 거래 원장을 변경하지 않으면서 별도 Demo signed 보호주문 GET 두 건, bounded signed 계정 스냅샷, owner별 사전 보호 계획, actualOrderId별 체결 세부내역을 함께 비교합니다. 정상 활성 두 레그/한 레그 발동 후 형제 취소 검토/부분 체결/양쪽 발동/고아 또는 외부 주문/관측된 청산을 구분합니다. 동일 Hedge 방향 복수 보호 그룹, 원본 변조, 타 포지션, 수량 불일치, 중복 체결, 시간이 지난 계정 증거는 모두 차단합니다.

**이 단계는 read-only 복구 *판정 라이브러리*만 구현했습니다.** 라이브 서버의 자동 보호주문 취소/재전송, 새로운 Binance Demo POST/DELETE, 영구 포지션 정산, 모델 PnL, HTTP 423 해제는 추가하지 않았습니다. 원자적 OCO를 주장할 수 없으며, signed REST의 제한된 조회기간은 전체 과거 거래 무결성을 증명하지 못합니다. PC 연결 시 별도의 서명 조회·실환경 검증이 필수입니다. 자세한 범위와 회귀 테스트: `docs/DEV385_PROTECTION_RECOVERY_KO.md`, `docs/DEV385_TEST_REPORT_KO.md`.

---

## v0.4.11-dev.3.84 — 패키지 A 2회차: 정산 무결성 차단 저널 (NON-POSTED)

3.83의 진입 체결 복구 사전계획과 기존 정산/이중 장부 비적용 단계를 연결하는 **영구 sidecar 저널**을 추가했습니다. signed entry archive 및 로컬 원본을 재검증한 뒤 `PREPARED_NOT_POSTED` → `FENCED_NOT_POSTED`를 checksum chain으로 보존하며 모델·포지션 방향·양쪽 trade IDs·누락 체결 후보를 고정합니다. 재시작, 중복 호출, 원본 변조와 잘못된 선행 단계에서 fail-closed합니다.

**이 릴리스는 실제 영구 금융 정산을 하지 않습니다.** 진입 체결 import, Lifecycle 전이, 모델 Ownership/PnL 변경, HTTP 423 해제는 미구현·금지입니다. 자세한 내용: [`docs/DEV384_SETTLEMENT_JOURNAL_KO.md`](docs/DEV384_SETTLEMENT_JOURNAL_KO.md).

---

## v0.4.11-dev.3.83 — 패키지 A 1회차: 진입 누락 체결 복구 사전계획 (읽기 전용)

`dev.3.82` 누적 소스에 기존 signed `entry-archive`를 검증한 뒤 오프라인에서
누락 진입 체결을 `TradeID`별로 준비·검증하는 `plan-entry-recovery`를 추가했습니다.
원본 장부·라이프사이클과 archived GET 상세를 여러 번 대조하고, 로컬에 이미
저장된 체결과 누락된 체결을 분류하며, 서명 사본/장부가 변하면 이전 plan은 무효입니다.
`pc-demo-package-a-collect.ps1`은 외부 청산 후보가 모호하더라도 진입 증거 다음에
별도 복구 **사전계획 JSON**을 먼저 보존합니다.

**중요:** 이는 실제 체결 파일 생성, `MAY_HAVE_SENT` 해소, 소유권/정산 적용이 아닙니다.
실제 Demo signed 재조회 및 전체 계정 이력·모델별 펀딩 검증 없이
`ImportDemoOrder`를 호출하지 않습니다. 거래/자동매매는 계속 HTTP 423 잠금입니다.
PC 사용법과 한계는 [`docs/DEV383_ENTRY_RECOVERY_PREFLIGHT_KO.md`](docs/DEV383_ENTRY_RECOVERY_PREFLIGHT_KO.md).

---

### dev.3.82 read-only one-click incident evidence collection

With the server stopped, local `data/` and the two-line `*.enc` credential file intact, and Go installed, run:

```powershell
.\scripts\pc-demo-package-a-collect.ps1
```

Or supply `-ClientOrderID` for another already-owned entry. The script only invokes the Demo signed GET inspector and writes unique JSON reports under `pc-results`; it never orders, settles or resets old `MAY_HAVE_SENT`. If there is no exactly-one independently confirmed external exit, it stops at review-only discovery and preserves the entry report. The local `pc-results/package-a-collection-*.json` summary shows produced files, failure or ambiguity and conservative flags. Historical retention and funding attribution remain blockers, even if all collection stages succeeded. Back up private evidence securely, do not share `.enc`.

## dev.3.81 — OFFLINE 사고 증거 진단·차단 사유 보고서 (READ ONLY)

`dev.3.80`의 `discovery-archive`가 저장한 사고 증거 묶음이 있는 경우,
**네트워크·Binance API 키 없이**, 현재 원본 거래 기록을 다시 검증하면서
어떤 정산 선행조건이 충족되지 않았는지 읽기 전용 진단을 생성할 수 있습니다.

```powershell
.\scripts\pc-demo-package-a.ps1 -ClientOrderID package-a-signed-YYYYMMDD_HHMMSS_fff.json -Action diagnose-discovery-archive
```

입력은 `pc-results`에 있는 **JSON 파일명만** 허용합니다. 외부 청산 후보를 독립적으로
검증하고 보존한 원본 증거가 없거나 로컬 장부가 달라졌다면 진단도 실패합니다.
보고서 `checks`, `blocker_codes`, `missing_local_entry_trade_ids`, `local_entry_lifecycle_stage`
항목을 검토하세요. 정상 진단도 `settlement_authorized=false`, `trading_authorized=false`입니다.
이는 **진입·청산 체결의 한정 구간 무결성 진단**이지, 전체 계정·펀딩 귀속·정산 완료가 아닙니다.
자세한 내용은 `docs/DEV381_OFFLINE_TRIAGE_KO.md`에 있습니다.

## dev.3.80 — ONE-STEP Discovery + Incident Archive (Demo signed GET, READ ONLY)

`exit-discovery` previously found the external/manual close **candidate order ID** but
required copying it into `incident-archive`. `discovery-archive` now performs BOTH
read-only audits with independent signed GETs and captures a single, checksum-sealed,
non-posting `pc-results/package-a-signed-*.json` file **only if the discovery
finds exactly one complete candidate and the second audit agrees**. If there
are multiple candidates, missing/expired history, non-flat account, corrupted
journal, changed signed order/fill values or other ambiguity, the command fails
closed without selecting an exit or producing a success report.

Server OFF, Demo API credentials stored locally in `.enc` (never included in
archive), and original `data/` preserved:

```powershell
.\scripts\pc-demo-package-a.ps1 -ClientOrderID bt_06893a0c50b6928dede7365cd2f0 -Action discovery-archive
```

When a bundle has been captured, independently check the returned candidate
order ID against Binance Demo records. For **offline integrity recheck**
(no API credentials or internet needed), provide the saved JSON **basename**:

```powershell
.\scripts\pc-demo-package-a.ps1 -ClientOrderID package-a-signed-YYYYMMDD_HHMMSS_fff.json -Action verify-discovery-archive
```

**Neither action proves that the discovered order was manually placed or
proves full account history / pooled Hedge funding allocation.** An exported
JSON with a local SHA-256 checksum is NOT cryptographically signed by Binance.
`MAY_HAVE_SENT`, Lifecycle, Ownership, wallet, real order routes and HTTP 423
remain unchanged. A completed archive is NOT settlement or a trading permit.
See `docs/DEV380_DISCOVERY_ARCHIVE_KO.md`.

## dev.3.79: Find manual exit order ID (Package A, READ ONLY)

With the Demo server **stopped** and the original `data/` and local credential `.enc` preserved, a user who does not know the external/manual close's exchange order ID can run:

```powershell
.\scripts\pc-demo-package-a.ps1 -ClientOrderID bt_06893a0c50b6928dede7365cd2f0 -Action exit-discovery
```

This only makes signed Demo GET requests and saves `pc-results/package-a-signed-*.json`. Review `exit_discovery.external_exit_order_candidates` and its `status` and `blockers`. **Never treat a candidate as proof that the user manually closed the order**. After independently checking the exact close order ID in Binance Demo, use `-Action incident-archive -ExitOrderID <VERIFIED_ID>` to preserve incident evidence. A missing, ambiguous, partial or too-old history is a **blocker**, not evidence of absence. All trading and financial posting remain LOCKED. See `docs/DEV379_EXIT_DISCOVERY_KO.md`.

## dev.3.78: Signed external-close incident archive (Package A, NON-POSTED)

This release captures both the original owned entry and an independently identified Binance Demo external manual close, including signed GET order/fill data, bounded BTCUSDT trade history and bounded funding rows into one locally checksum-sealed, offline-verifiable report. It never imports fills, changes the original execution lifecycle, changes Ownership, certifies account-wide history or attributes pooled Hedge funding.

With the server stopped, on Windows run (substitute the actual signed external close **exchange order ID**, do not guess):

```powershell
.\scripts\pc-demo-package-a.ps1 -ClientOrderID bt_06893a0c50b6928dede7365cd2f0 -Action incident-archive -ExitOrderID <SIGNED_CLOSE_ORDER_ID>
```

To verify the produced report without network or credentials, run `-Action verify-incident-archive -ClientOrderID <package-a-signed-...json>` (the report's basename). `entry-archive` is still available when no verified close order ID exists. The historical signed GET API can reject records outside its retention period; never treat a not-found as proof of no trade.

**Package A is NOT financially completed:** an end-to-end crash-safe financial posting protocol, account-wide history completeness and independent pooled-Hedge funding attribution require live Demo evidence and operational review. Orders remain HTTP 423 LOCKED.

## v0.4.11-dev.3.77 — Demo signed entry evidence archive and offline source audit (LOCKED)

- `-package-a-action entry-archive`: capture a **full signed entry order and fill-detail snapshot**, re-read independently and compare with the local entry-gap report before writing a sealed, append-only JSON file under `pc-results/`. No API keys or secrets are serialized. This is deliberately only a bounded *single entry order* snapshot, not an entire trading account ledger.
- `-package-a-action verify-entry-archive`: offline recheck archive integrity and original immutable entry journal with no Binance connection or credentials; output a fresh non-posting verification JSON. The snapshot is an exported server response, **not a Binance-signed artifact**. Local SHA-256 only detects accidental/unauthorized local changes when trusted copies of the hash are retained separately.
- Existing risk controls, old SHORT `MAY_HAVE_SENT`, source journals and automatic order lock remain untouched. No settlement/apply/import routes added.

## v0.4.11-dev.3.76 — Package A 통합 signed 재검증·비적용 복구 체크포인트 (LOCKED)

- 기존 원본 entry fill이 없어 정산 제안이 차단되는 사건을 위한 독립 signed GET `entry-probe`도 추가했습니다. 이 도구는 누락 체결 ID만 보고하며 실제 복구는 하지 않습니다.
- 기존 dev.3.75 이중 장부 봉인을 바탕으로 **실제 Demo signed GET을 재실행**해 진입/청산의 가격·수량·수수료·실현손익, 체결 ID와 시간 순서를 sealed 원장과 대조합니다.
- 계정의 제한된 기간별 펀딩 원본 행·중복 ID·총합까지 검사하고, 증거 범위를 `ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY`라고 명시합니다. 이 검사는 **전체 이력 완전성/모델별 펀딩 귀속을 증명하지 못합니다.**
- 누락된 원본 entry fill과 외부 exit fill을 분리하여 보고하되 **원래 체결 파일을 생성하거나 정산하지 않습니다**.
- `internal/trading/package_a_preflight.go`: append-only `PACKAGE_A_PREPARED_NOT_POSTED → PACKAGE_A_RECHECKED_NOT_POSTED` 기록, 원본 Lifecycle/Ownership snapshot 및 signed 지문 검증, 동시 재실행 및 중단 기록 차단.
- `scripts/pc-demo-package-a.ps1`: `-ClientOrderID ... -Action probe|stage|recheck` (Demo signed GET만 사용; server OFF; 계정 비밀번호/키 JSON 미기록). 사전 dev.3.68~3.75 증거/검토/dual-ledger 검증 모두 통과해야 작동합니다.
- **실제 `MAY_HAVE_SENT` 해소·외부 청산의 금융 정산·펀딩 귀속·모델별 확정 PnL·자동매매 활성화는 미완료입니다.** PC 실증거를 얻기 전 계속 HTTP 423 잠금을 유지합니다. 파일 해시는 거래소의 서명이 아니며 독립적인 규제 수준 정산 승인이 아닙니다.

## v0.4.11-dev.3.75 — Package A NON-POSTED dual-ledger reconciliation envelope (LOCKED)

- Added `ExternalCloseDualLedgerPlanStore`: one checksummed envelope binds the unmodified execution-lifecycle evidence, missing/uncertain lifecycle status, original ownership/fills checksum and signed Shadow Replay into *two joint projected actions*. No real mutation.
- Two exclusive, fsynced stages (`DUAL_LEDGER_PREPARED_NOT_POSTED` → `DUAL_LEDGER_VERIFIED_NOT_POSTED`) with original full dependency validation on every load, deterministic checksums and fail-closed torn/orphan/unknown/symlink detection.
- Added offline CLI + PowerShell wrapper, LONG/SHORT tests for 30 simultaneous calls, crash/restart, evidence drift and forged posting flags. Original `MAY_HAVE_SENT` and HTTP423 remain intact.
- **Not a financial commit**: no atomic write to actual Lifecycle/Ownership, signed funding allocation, all-history proof or trading authorization. See `docs/DEV375_DUAL_LEDGER_ENVELOPE_KO.md`.

## dev.3.74 — Package A crash-safe shadow posting simulation (LOCKED)

New `external_close_shadow_ledger.go` and offline `scripts/pc-demo-close-shadow-replay.ps1` support deterministic LONG/SHORT shadow `PREPARED_NOT_POSTED → REPLAYED_NOT_POSTED` records based on a globally reserved posting intent. The staged simulator binds IDs/amounts, checks provisional net excluding funding and persists an immutable replay marker; interrupted/restarted calls remain fail-closed and idempotent. **This is NOT a settlement implementation:** no original Lifecycle, Ownership, fills, model PnL, trade lock or `MAY_HAVE_SENT` is changed. Full signed historical coverage and pooled Hedge funding allocation remain unresolved. See [`docs/DEV374_SHADOW_CLOSE_REPLAY_KO.md`](docs/DEV374_SHADOW_CLOSE_REPLAY_KO.md).

## dev.3.73 — Package A signed bounded coverage + globally exclusive non-posted posting intent

`DemoSignedCloseReader` reports the entry-first to exit-last signed audit window and counts and blocks interleaved fills and zero-price fills. After dev.3.72 `STAGED_NOT_POSTED → RECHECKED_NOT_POSTED`, an offline explicit reservation of a **single globally exclusive non-posted transaction identity** is supported by `scripts/pc-demo-close-posting-intent.ps1`. This is a future double-allocation fence only; it **does not post** fills, clear `MAY_HAVE_SENT`, verify full historical funding, enable auto trading or unlock HTTP423. See [`docs/DEV373_PACKAGE_A_NONPOSTING_INTENT_KO.md`](docs/DEV373_PACKAGE_A_NONPOSTING_INTENT_KO.md).

## dev.3.72 — Package A / signed revalidation and non-posting checkpoint

Binance Demo signed GET integration now checks entry/exit FILLED orders twice, exact fills, bounded all-account BTCUSDT trades and funding income. Evidence changes invalidate replay keys. A separate append-only STAGED → RECHECKED checkpoint can resume after restart; **it cannot and does not post external closes, finalize PnL or clear MAY_HAVE_SENT**. HTTP423 order lock remains. See [`docs/DEV372_PACKAGE_A_SIGNED_REPLAY_KO.md`](docs/DEV372_PACKAGE_A_SIGNED_REPLAY_KO.md). Actual Binance Demo E2E and full funding/historical audit are pending.

## dev.3.70 — 외부 청산 정산 적용 사전 게이트 (읽기 전용, LOCKED)

## dev.3.69 — 외부 청산 정산 조정안 (미적용, LOCKED)

검토 완료된 signed 외부 청산 **후보**를 별도 `external_close_settlement_proposals/proposal.json` 미적용 조정안으로 기록할 수 있다. 이 파일은 원본 계정·모델 소유권·체결·Lifecycle·손익을 고치지 않고, 펀딩과 전체 이력 검증도 완료하지 않는다. `posting_applied=false`, `trading_authorized=false`, HTTP 423 잠금 유지. 자세한 내용은 `docs/DEV369_EXTERNAL_CLOSE_SETTLEMENT_PROPOSAL_KO.md`.

## dev.3.68 — 운영자 외부 청산 검토 append-only 장부 (LOCKED)

기존 signed 외부 청산 *리허설*을 별도 기록으로 보존하는 sidecar `external_close_reviews`를 추가했다. `REVIEW_STAGED → OPERATOR_ACKNOWLEDGED_REVIEW_ONLY / OPERATOR_REJECTED_REVIEW_ONLY`만 허용한다. 지문·원본 변경·중복 호출·디스크 손상/경합을 엄격히 검사하지만, **저장된 JSON 또는 운영자 확인은 거래소 signed 인증이나 정산 허가가 아니다**. 기존 `MAY_HAVE_SENT`, 예약, 체결, 소유권, 거래 잠금은 그대로다. 상세 `docs/DEV368_APPEND_ONLY_REVIEW_LEDGER_KO.md`.

## dev.3.67 — LONG/SHORT 외부 청산 비파괴 복구 리허설 (Demo READ-ONLY, LOCKED)

`dev.3.66`의 signed 외부 청산 후보 판정에 **메모리 기반 수량·수수료·손익 리허설**과 지문 검사를 추가했다. `scripts/pc-demo-ledger-audit.ps1`에서 만들어지는 JSON `external_close_rehearsals`를 확인한다. 이는 거래 허가, 최종 PnL 확정, 실제 거래 장부 복구가 아니다. 기존 `data/`와 암호 파일은 절대 삭제하지 마라. 자세한 내용은 `docs/DEV367_RECOVERY_REHEARSAL_KO.md`에 있다.

## dev.3.66 — 외부 청산 후보 LONG/SHORT 대조 (Demo READ-ONLY, LOCKED)

서버를 정지하고 `scripts/pc-demo-ledger-audit.ps1`을 실행하면 `pc-results/signed-ledger-audit-*.json`에서 `external_close_recovery_cases`를 확인할 수 있습니다. 후보 판정은 **정산 또는 거래 허가가 절대 아니며**, 로컬 진입 체결 누락·미종료 Lifecycle/예약/과거 이력 제한을 보고서에 명시합니다. LONG/SHORT 공통 진입·청산 방향, signed 체결 합계, fresh flat account 등 모든 조건을 검증합니다. 상세 `docs/DEV366_EXTERNAL_CLOSE_RECOVERY_CANDIDATES_KO.md`.

## dev.3.65 — Demo 개별 주문/체결 signed 감사 (실제 주문 잠금)

`dev.3.64`의 현재 계정/로컬 대조에 추가하여 **로컬 주문별 signed GET 재확인과 외부 BTCUSDT 체결 식별** 기능을 제공합니다. 프로젝트 루트의 두 줄짜리 Demo API 키 `.enc` 파일(로컬에만 보관)이 필요합니다. 트레이더 서버를 정지하고 `scripts/pc-demo-ledger-audit.ps1`을 실행하면 `pc-results/signed-ledger-audit-*.json`에 결과만 저장합니다. **명령어는 signed GET만 사용하며 자동매매·수동매매 주문이나 로컬 정산을 수행하지 않습니다.** `BLOCKED_REVIEW_REQUIRED`는 정상적인 안전 응답일 수 있습니다. 충분한 독립 이력이 확인되더라도 결과는 `SIGNED_LEDGER_FORENSICS_REVIEW_ONLY`이며, 거래 허가를 의미하지 않습니다.

기존 SHORT를 바이낸스 웹에서 수동 청산한 사실은 외부 `BUY/SHORT` 체결 후보로 감지할 수 있지만, 과거 거래 이력 전범위와 수수료·펀딩·모델 귀속 및 미정산 `MAY_HAVE_SENT`의 자동 복구는 하지 않습니다. 계정 권한은 Demo 읽기 전용 범위로 취급하십시오. 자세한 설명은 [DEV365_SIGNED_LEDGER_FORENSICS_KO.md](docs/DEV365_SIGNED_LEDGER_FORENSICS_KO.md).



### dev.3.70 external-close posting preflight

`trading.ExternalCloseSettlementProposalStore.LoadExternalClosePostingGate` returns a deterministic, read-only, non-authorizing checklist. All seven prerequisites remain blocked until independently verified; there is no live posting or settlement mutation in this release.

### dev.3.71 signed revalidation boundary

`internal/trading.RevalidateExternalClose`는 외부 증거 조회 인터페이스와 안정적인 replay key를 제공한다. 제공된 읽기 전용 인터페이스만으로 실제 signed 응답, 전체 거래 내역, 펀딩 및 적용 원자성을 증명하지 못한다. 따라서 결과는 반드시 `REVALIDATED_EVIDENCE_POSTING_STILL_BLOCKED`이고 주문 및 정산은 계속 잠금 상태다.


### 3.105 read-only exit/owner exposure review
`internal/trading/protection_exit_owner_exposure.go` compares trusted signed exit inspection, the immutable TP/SL owner plan, a separately verified pre-exit local owner snapshot, and a bounded account-wide REST observation. It never reconstructs full ownership, never settles any ledger and never enables orders. Hedge same-side owners are pooled on Binance; matching aggregate quantity does not establish attribution. The function is not wired to the product server.


### dev.3.122: 외부 청산 금액 대조 (오프라인, 원장 비적용)

보관된 Package A 외부 청산 사건과 기존 로컬 정산 제안을 수량/수수료/실현손익 단위로 교차 검증합니다. 펀딩은 모델에 임의 귀속하지 않고 미배분으로 유지합니다. `go run ./cmd/demo-close-financial-review -data ./data -id <실제 ID> -incident <PackageAIncidentArchive.json>`를 사용합니다. 자세한 설명은 `docs/DEV3122_OFFLINE_FINANCIAL_AMOUNT_RECONCILIATION_KO.md`입니다. 실원장 금융 COMMIT 미구현, 제품 및 Binance 주문 경로 HTTP 423 LOCKED 유지.

### dev.3.121: Binance Demo 보호주문 signed 읽기 전용 감사 (PC 검증 전)

서버를 종료한 Windows PowerShell 5.1에서 `Get-Content .\VERSION`이 `v0.4.11-dev.3.121`인지 확인한 뒤, 이미 존재하는 예약 ID로만 다음 명령을 실행합니다:

```powershell
.\scripts\pc-demo-protection-signed-audit.ps1 -ClientOrderID 'bt_<실제 28자리 hex>' -WindowHours 24
```

3.121은 기존 `.enc`(2줄: API key / secret)를 비밀로 읽고, 바인딩된 로컬 원장과 Binance Demo signed GET을 **검토**합니다. `pc-results`에 오류 발생 시에도 덮어쓰지 않는 JSON을 기록하며, **주문/취소를 하지 않습니다**. 정상 검토 결과조차 `SIGNED_BOUNDED_OWNER_MATCH_NOT_APPROVED`, 모든 거래 승인 false, `HTTP 423 LOCKED`를 유지합니다. 자세한 제한은 `docs/DEV3121_PC_DEMO_PROTECTION_SIGNED_AUDIT_KO.md`를 참조하세요.
