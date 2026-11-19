## v0.4.11-dev.3.138 — 장시간 모의 다중 모델 위험·장애 주입 감사 (HTTP 423 LOCKED)

- `AuditOfflineMultiModelSoak`는 3.137의 signed Equity 위험 미리보기와 영속 RUN/STOP 세대를 **오프라인 다중 캔들 추적**으로 반복 평가. 실제 체결/주문 실행 없이 후보·중복 억제·증거 실패·Equity 차단을 분리 집계.
- 테스트용 가상 시계로 24시간 1,440개의 1분봉과 96개 중복 이벤트를 재생. WS 수신 지연 및 signed 계정 증거 불일치에서 차단, 같은 분 내 증거 복구 시에만 재검토; 1분봉 유실/역행은 추적 전체에 `GAP_REQUIRES_RECONCILIATION`으로 고정하고 자동 연결 복구 후에도 후보 생성 금지.
- STOP은 신규 진입 후보만 차단하고 소유 포지션은 그대로 둠. 프로세스 재시작으로 이전 RUN ID를 소유하지 않는 경우에도 `RESTART_BLOCKED` 유지. 손상된 제어 파일 및 사전 취소 Context 차단.
- `scripts/pc-demo-multi-soak-offline-tests.ps1`, `docs/DEV3138_OFFLINE_MULTIMODEL_SOAK_KO.md` 추가. 이는 **가상 시간의 시뮬레이션**이며 실제 24시간 실시간 E2E, Binance private WS, 모델 성능·체결·확정 PnL 검증이 아님. 제품 START/STOP/주문 HTTP 423 유지.

## v0.4.11-dev.3.137 — PC 없는 다중 모델 증거금 한도·STOP 세대 통합 오프라인 감사 (HTTP 423 LOCKED)

- `ReadOnlyMultiModelScheduler.ReviewOfflineMultiModelRiskCycle` 추가. 기존 다중 모델 신호 미리보기·공유 지갑 예산과 `AutoRunControl`의 영속적 STOP/RESTART_BLOCKED 세대 상태를 **읽기 전용**으로 연결. STOP/재시작 차단 시에는 신규 신호 평가 자체를 수행하지 않으며 기존 포지션·TP/SL·원장을 수정하지 않음.
- Binance Demo signed 전체 계정 관측(`RecoveryBaseline`의 AccountPortfolio)과 기존 fresh signed Snapshot을 USDT 지갑·가용 잔고·Hedge 방향 수량·자산·미체결 주문 목록·5초 시간 범위로 대조. 자료가 없거나 변하면 신규 위험 미리보기를 거부. 계정 개설 이후 전체 이력이나 원자적 스냅샷을 입증하지는 않음.
- 전체 Equity(`TotalMarginBalance`) 대비 **자동 모델의 기존 추정 증거금 + 미종료 예약 + 이번 미리보기 증거금 ≤ 정책 1~30%**를 `big.Rat`로 검증. 초과 후보만 `BLOCKED_EQUITY_CAP`으로 제외하고 한도를 강제 축소하거나 기존 포지션을 닫지 않음. 모델별 10% 예산·15배 ISOLATED·같은 Hedge-side 동시 진입 방지 등 기존 보수적인 미리보기 계약을 재사용. **정책 변경 UI 및 실주문 경로는 연결하지 않음**.
- 모델 평가 도중 STOP/세대 변경 시 후보를 통째로 폐기. 동일 캔들 중복 미리보기 차단, 다른 프로세스의 RUNNING 복원 차단, 기존 증거금이 정책 한도를 초과하더라도 신규 진입만 금지함을 독립 테스트로 검증.
- 테스트: 30%/15%/9%/8% 경계, LONG/SHORT 모델, 기존 보유 포지션, STOP/비정상 재시작, 평가 중 STOP 경합, 손상된 제어 파일, 불완전한 Portfolio·잔액·자산·포지션, 중복 캔들/동시 호출, 취소된 Context. `scripts/pc-demo-multi-risk-offline-tests.ps1` 테스트 전용 스크립트 제공.
- **오프라인 읽기 전용 위험 진단**이며 LIVE signed 조회, Binance 주문/취소, 모델별 확정 정산, 무인 다중 모델 거래, HTTP 423 잠금 해제는 하지 않음. 현재 등록 모델은 기존 EMA/RSI/MACD 2개뿐이고 첫 실제 주문 시도 후 전체 감독형 중단 제약은 유지. 상세: `docs/DEV3137_OFFLINE_MULTIMODEL_RISK_STOP_KO.md`.

## v0.4.11-dev.3.136 — 읽기 전용 실제 모델 레지스트리·선택 정합성 감사 (HTTP 423 LOCKED)

- **실제 배포 소스의 모델 수는 2**: `ema_rsi_macd_v1`, `ema_rsi_macd_strict_v1`. 이전에 언급된 Logistic 모델 5개의 소스나 검증된 아티팩트는 등록되어 있지 않으며 이 둘과 동치로 취급하지 않음.
- `app.AuditModelConfiguration`: UI catalog와 동결된 실행 registry의 ID·표시 필드, 선택된 모델의 실존·유일성·개수를 검사. `NO_MODEL_SELECTED`, `CONFIG_CONSISTENT_TRADING_LOCKED`, `BLOCKED_CATALOG_OR_SELECTION_MISMATCH`를 반환. 실제 Binance 상태, 모델 정확도, 확정 PnL 또는 주문 승인을 주장하지 않음.
- `/api/v1/models/runtime`에 별도 `model_registry_audit` 보고서 추가. 기존 `preview_configuration_ready`의 선택 검증 누락 가능성을 닫고, 등록/선택 불일치 시 준비 상태 false. 다중 모델은 공유 계정·지갑·수명주기, Hedge-side 점유 직렬화, **첫 주문 시도 후 전체 감독형 종료** 제약을 유지함.
- `cmd/model-registry-audit -selected ...` 또는 `-settings ...` (오프라인, 읽기 전용) 및 `scripts/pc-demo-model-registry-audit.ps1` 추가. 잘못된 JSON, 추가 값, symlink 파일, 중복/없는 모델, catalog drift, 권한 플래그 위조를 회귀 검증.
- **범위 제한**: 개발/검증용 구조 진단이며 시장 성과·실제 Binance 계정 소유권·실행 5모델 기능이 확인된 것은 아님. HTTP 423 LOCKED, 금융 COMMIT·실주문 미연결 및 Windows/Demo 실검증 보류. 문서: `docs/DEV3136_MODEL_REGISTRY_AUDIT_KO.md`.

## v0.4.11-dev.3.135 — STOP·재시작 시 보호주문 모의 장애 안전성 보강 (HTTP 423 LOCKED)

- `RehearseRecoveredEntryToProtectionOffline`의 `INSPECT_ONLY`가 계획이 없을 때 signed 체결 복구·로컬 계획 생성까지 수행하던 부작용 수정. 이제 `ENTRY_UNPREPARED_READ_ONLY_REVIEW`로 보수적 분류하고, 분리된 명시적 `ARM` 모의 동작에서만 계획을 준비합니다. 반복 조회·재시작해도 진입 Lifecycle 및 보호계획 불변.
- 하위 `RehearseEntryProtectionDispatch`의 미지원 action은 signed GET 이전에 거부하고 nil context를 차단. 오프라인 모의 거래소의 승인 경계 유지.
- STOP/서버 재시작 사고 테스트: 기존 `AutoRunControl`의 generation/lease 무효화 및 `RESTART_BLOCKED`를 확인하고, 기존 LONG/SHORT 보호 레그는 강제 청산하지 않고 signed 조회를 허용함. 발동된 leg의 **별도 명시적** 모의 형제 취소, 부분 청산·이중 발동 시 자동 취소/재발행 금지를 확인.
- DELETE가 모의 거래소에는 적용되었으나 ACK만 유실되는 조건을 새로 재현하여 재시작 시 signed-only 복구 및 DELETE 재전송 0회를 검증. 기존 ACK 유실(삭제 불확실) 경로도 유지.
- 이 모든 검증은 오프라인 테스트용 adapter에 한정. **실제 Binance TP/SL POST·DELETE, 자동 보호주문 감독 루프, 회계 COMMIT, 자동매매 잠금은 변경하지 않음.** 실제 Binance Demo/Windows E2E 미검증. 상세: `docs/DEV3135_OFFLINE_STOP_PROTECTION_INCIDENTS_KO.md`.

## v0.4.11-dev.3.134 — 오프라인 진입 복구부터 TP/SL 모의 Lifecycle E2E (HTTP 423 LOCKED)

- `RehearseRecoveredEntryToProtectionOffline`: 기존 signed-only 진입 체결 복구·원장 소유권 검증·TP/SL 계획 준비를 3.119의 **비공개 sealed 모의 거래소** 발행/복구·형제 취소 흐름에 연결. 실제 거래소 클라이언트는 모의 인터페이스를 구현할 수 없으며 HTTP/무인 런타임에서 호출하지 않음.
- 이미 준비된 계획은 재생성하거나 청산된 진입을 다시 복구하지 않고 기존 signed lifecycle을 재확인. 미체결/부분체결은 계획·발행 중지, 불명확/손상 계획은 fail-closed. 최초 진입 복구, LONG/SHORT pair, TP 청산, sibling 단발 취소, 재시작 후 POST/DELETE 재전송 0회를 종단간 테스트.
- TP ACK/DELETE ACK 유실 시 `MAY_HAVE_SENT`가 유지되고 **signed GET 복구만** 허용. 단일 레그·부분청산·이중 발동·외부 Algo 개입·위조 상태·기타 오류 차단 테스트. 확정 손익·금융 COMMIT·정산/거래 승인 없음.
- 전체 PC·Binance Demo E2E 및 실제 보호주문 실행은 아직 미검증; 제품 자동매매 API **HTTP 423 LOCKED** 유지. 상세: `docs/DEV3134_ENTRY_TO_PROTECTION_OFFLINE_E2E_KO.md`.

## v0.4.11-dev.3.133 — 모델별 비용 집계 정책 및 펀딩 미귀속 오프라인 감사 (HTTP 423 LOCKED)

- `InspectExternalModelAttributionPolicy`는 2~64개의 독립 오프라인 사건을 기존 교차 사건 감사로 다시 확인하고 각 사건의 **진입/청산 체결 수수료·실현손익·펀딩 거래를 원본에서 재검증**합니다. 주문/체결/펀딩 ID 재사용 및 같은 Hedge 방향 보유기간 충돌은 단순 합산하지 않고 차단합니다.
- 지역 모델 ID별로 **bounded provisional** 진입/청산 수수료, 거래소 실현손익, 펀딩 제외 순손익을 `big.Rat`로 합산합니다. 최종 모델 손익은 언제나 `null`입니다. 계정 펀딩은 사건별 관측값만 유지하며 `model_funding_usdt:null`, `account_wide_funding_usdt:null`, `funding_allocated:false`를 보장합니다. 펀딩 합계가 정확히 0이어도 모델 펀딩 0을 **확정하지 않습니다.**
- 반환 리포트는 출처별 digest, 모델별 사건 ID, 필수 BLOCKED 요구조건을 포함하지만, 로컬 SHA-256만으로 교환소 보관 이력, 동일 Binance 계정 귀속 또는 모델 소유권을 증명하지는 못합니다. 읽기 전용 보고서 검사 중 원본 증거 변동 시 차단합니다.
- `cmd/demo-close-cross-event-audit -manifest ... -model-policy`와 `scripts/pc-demo-model-attribution-policy.ps1` 추가. LONG/SHORT, 두 사건 수수료 불변식, 펀딩 미귀속, 중복 ID·변조·재해시된 금액/승인 주장, 재시작 순서 독립성, 읽기 전용 결과 테스트.
- 실제 금융 COMMIT, 모델별 확정 손익, TP/SL 거래소 실행, 자동매매 API **전부 비활성/HTTP 423 LOCKED**. Windows PC 설치 및 Binance signed Demo GET 미검증. 문서: `docs/DEV3133_OFFLINE_MODEL_COST_POLICY_KO.md`.

## v0.4.11-dev.3.132 — 모의 원자적 COMMIT 중단·재시작 안전성 (HTTP 423 LOCKED)

- 3.131 모의 저장소에 읽기 전용 `InspectRecovery`와 **증거 SHA-256 및 이전 단계 수 고정** `ResumeNext`를 추가. 외부에서 본 진행 상태와 다르면 다음 단계를 실행하지 않고 재검사를 요구합니다.
- 각 단계 직전 중단, 기록 완료 후 응답 유실, `COMMIT` 전후 재시작, 빈 파일·잘린 기록·최종 COMMIT 변조, 원본 변화, 동일 복구 요청 동시 제출을 결정론적 모의 장애 테스트로 검증합니다. 불완전 파일은 자동 삭제/재작성하지 않습니다.
- `demo-close-mock-financial-commit` CLI 및 PowerShell 도구에 `recover`(읽기 전용), `resume-next`(확인 토큰과 `completed_steps`/`consistency_sha256` 핀 필수) 추가. 모든 결과는 **오프라인 모의 원장만** 대상으로 하며, 금융 COMMIT·거래·승인 기능은 열지 않습니다.
- 실제 Windows 정전·데이터 디스크 내구성·Binance Demo E2E 미검증. 상세: `docs/DEV3132_MOCK_COMMIT_RESTART_RECOVERY_KO.md`.

## v0.4.11-dev.3.131 — 격리 오프라인 3원장 단일 커밋 경계 시뮬레이터 (HTTP 423 LOCKED)

- `ExternalMockFinancialCommitStore`: 전역 예약 및 사건별 `SEALED` 원자적 리허설을 **매 단계 재검증**한 뒤 원본과 다른 기존 디렉터리에 `INTENT → LIFECYCLE → OWNERSHIP → FINANCIAL → COMMIT` 불변 JSON을 `O_EXCL` + `fsync`로 순서대로 기록하는 오프라인 모의 엔진.
- 최종 `COMMIT` checkpoint가 유효하기 전에는 세 Ledger projection을 **모두 비공개**; 완성되면 세 뷰가 같은 배치/해시의 **모의 읽기 모델**로만 함께 표시됩니다. 중복 요청은 멱등, 중간 기록 손상·누락·단계 건너뛰기·원본 데이터 변경·디렉터리 겹침·동시 경쟁은 fail-closed.
- CLI `cmd/demo-close-mock-financial-commit` (`inspect|advance`), `scripts/pc-demo-mock-financial-commit.ps1` 추가. `advance`는 별도 `SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT` 확인 문자열을 요구합니다. Binance 네트워크/인증/주문은 호출하지 않습니다.
- **실제 금융 원장 COMMIT이 아닙니다.** 모델별 수수료·펀딩 확정 귀속이나 금액의 변동 없이 원본 거래·소유권·지갑 원장은 불변, 실제 정산/거래 승인 flag는 항상 false. 3.130 서명 검토 ACK도 금융 승인 토큰으로 취급하지 않습니다. 파일 삭제·외부 저장소 오염 등은 별도 신뢰 저장소가 필요. `HTTP 423 LOCKED` 유지.
- 검증: 오프라인 2사건 LONG/SHORT, 최초/중간/최종 단계 가시성, 재시작 멱등, 훼손·결번·원본 변경, 동시 실행/루트 겹침 테스트. 상세 `docs/DEV3131_MOCK_MULTI_LEDGER_ATOMIC_COMMIT_KO.md`.

## v0.4.11-dev.3.130 — 오프라인 독립 서명 검토 증거·nonce 재사용 차단 (HTTP 423 LOCKED)

- `ExternalIndependentApprovalRequest`: 3.125 전역 예약·실험적 원자적 봉인/금융 preflight을 사건별 정확한 해시에 고정한 **비승인 검토 요청**. 32바이트 nonce·유효기간(최대 24시간)·서명 대상 유일성을 검증합니다.
- `ExternalIndependentReviewAttestation`: 독립적으로 고정한 Ed25519 공개키 SHA-256과 분리된 서명 바이트 도메인을 확인합니다. 요청·모델·봉인·기간·결정·서명 위조를 거부합니다.
- `ExternalIndependentReviewReceiptStore`: 독립 기존 디렉터리에 `O_EXCL`+`fsync`로 일회성 nonce 사용을 기록합니다. 재시작/동시 사용 차단; 중단된 기록은 수동 검토가 필요하며 원본 원장은 수정하지 않습니다.
- CLI/PowerShell: `cmd/demo-close-independent-review`의 `intent|verify-once`, `scripts/pc-demo-independent-review.ps1`. 외부 서명 private key를 보관하거나 생성하지 않습니다.
- **독립 서명 검증은 실제 금융 승인이나 계정 전체 signed 증거가 아닙니다.** 확정 손익·원자적 실원장 COMMIT·자동매매 활성화 없이 HTTP 423 LOCKED 유지. 자세한 내용 `docs/DEV3130_INDEPENDENT_OFFLINE_REVIEW_ACK_KO.md`.

## v0.4.11-dev.3.129 — 오프라인 수집 구간 기준선·정산 권한 격리 검증 (HTTP 423 LOCKED)

- `PrepareExternalHistoryBaselineClaim`은 Package A incident의 원래 BTCUSDT 진입 첫 체결~청산 마지막 체결 시각, 오프라인 소스 SHA-256, `USER_TRADES`/`FUNDING_FEE` 두 관측 데이터셋만을 고정한 로컬 기대 JSON을 생성합니다. 사용자 작성 기대값이 계정 전체, 다른 종목, private stream 연속성, 모델 펀딩 배분, 정산 권한 또는 다른 기간을 주장하면 재해시해도 차단합니다.
- `InspectExternalHistoryBaseline`은 3.128 불변 체크포인트·최종 봉인·3.127 한정 구간 연속성 및 3.126 모델 재무 증거를 다시 조회해 대조합니다. 부분 수집은 `INCOMPLETE`, 완료되어도 `BOUNDED_CAPTURE_BASELINE_MATCHED_NOT_ACCOUNT_ATTESTED`로만 분류하며 전체 계정 이력, 모델별 확정 손익, 펀딩 귀속, 실원장 COMMIT 또는 거래 승인은 항상 false입니다. 검사 전후 원본 증거/체크포인트 재검사를 수행하고 소스에는 기록하지 않습니다.
- `cmd/demo-close-history-capture -action claim|baseline` 및 `scripts/pc-demo-signed-history-capture.ps1 -Action claim|baseline -BaselineClaimPath ...` 추가. LONG/SHORT, 부분/완료, 재시작/동시 조회, 재해시된 확대 범위 주장, 봉인/체크포인트 제거, 권한 플래그 변조 테스트를 추가했습니다. 실거래소 signed GET 및 Binance 계정 전체 기준선 확보는 미실시·미증명입니다.
- **실제 금융 COMMIT, 모델별 확정 순손익, TP/SL 자동 발행/취소, 자동매매 API 활성화 없음** (`HTTP 423 LOCKED`). 문서: `docs/DEV3129_OFFLINE_HISTORY_BASELINE_GATE_KO.md`.

## v0.4.11-dev.3.128 — 오프라인 구간별 페이지 재수집·불변 체크포인트 (HTTP 423 LOCKED)

- `ExternalHistoryCaptureStore`는 오프라인 모의 signed GET 거래·펀딩 응답을 최대 24시간 미만으로 조회하고, 1,000건 한도 도달 시 요청 구간을 이등분합니다. 두 응답이 모두 존재하고 1,000건 미만일 때만 `O_EXCL`+`fsync`로 불변 checkpoint를 기록하며, 실패/재시작 시 다음 미수집 millisecond부터 진행합니다.
- header에 원본 incident/소스 로컬 SHA-256을 고정하고, 모든 페이지 기록을 이전 checksum으로 연결합니다. 원장/페이지 변경, 누락·순번 손상·응답 유실·1ms 단위 포화·타 소스 혼합을 차단합니다. 완료 후 기존 3.127 연속성 감사와 대조하여 최종 seal 생성; 체크포인트 변조/삭제는 fail-closed입니다.
- `cmd/demo-close-history-capture` 및 `scripts/pc-demo-signed-history-capture.ps1`의 `capture|inspect` 오프라인 실행 경로와 LONG/SHORT, 중단·재시작·변조·동시성 테스트. 실제 Binance signed GET, credentials, 주문 발행/취소, 금융 원장/COMMIT 연결 없음. `HTTP 423 LOCKED`. 상세: `docs/DEV3128_OFFLINE_HISTORY_CAPTURE_CHECKPOINT_KO.md`.

## v0.4.11-dev.3.127 — 제한된 signed GET 이력 구간 연속성·누락 차단 오프라인 계약 (HTTP 423 LOCKED)

- `InspectExternalSignedHistoryContinuity`는 기존 3.126 모델 수수료/펀딩 감사를 다시 검증하고, 별도로 보관한 **BTCUSDT userTrades + FUNDING_FEE signed GET 응답의 구간별 로컬 JSON manifest**와 사건의 원본 Package A incident를 교차 대조합니다. 응답은 실제 거래소에 다시 요청하지 않고 오프라인 자료만 사용합니다.
- 밀리초 단위 **구간 시작·종료가 정확히 연속**이고 첫 진입/마지막 청산 시각에 일치해야 하며, 24시간 초과 창, 동일 체결/펀딩 ID의 재출현, 누락된 응답(`null`), 1000건 한도 도달/잘림 의심, 거래·펀딩의 수량·금액·시각·종목 변조, 부족/추가/불일치 행과 로컬 원장 drift를 차단합니다. manifest/checksum은 로컬 SHA-256일 뿐입니다.
- 성공 결과는 `BOUNDED_WINDOW_SEQUENCE_MATCHED_NOT_ACCOUNT_ATTESTED`이며 관측된 *해당 구간*의 행/연속성 대조만 뜻합니다. **계정 전체 과거 거래, 다른 심볼, 미관측 일반 주문, private stream 연속성, Binance가 서명한 보관 증거, 모델 소유권·펀딩 귀속은 입증하지 못합니다.** 거래·정산·실원장 COMMIT·자동매매 승인 flag는 항상 false입니다.
- `cmd/demo-close-financial-review -model-evidence -history-windows <manifest.json>` 및 `scripts/pc-demo-signed-history-continuity.ps1` 추가. LONG/SHORT 정상, 펀딩 양/음, gap/overlap/역순/중복, 재해시된 위조 및 원본 변조, 재시작·동시 조회 회귀 테스트. 프로덕션 signed GET 수집·실행 연결은 다음 단계이며 **HTTP 423 잠금 유지**. 자세한 내용은 `docs/DEV3127_SIGNED_HISTORY_CONTINUITY_KO.md` 참고.

## v0.4.11-dev.3.126 — 모델별 수수료·펀딩 증거 계약 및 불변식 오프라인 감사 (HTTP 423 LOCKED)

- `InspectExternalModelFinancialEvidence`: 3.122의 검증된 bounded signed incident + original ledger + external-close proposal을 **다시 검증**하고, 각 진입·청산 체결의 거래 ID/주문 ID/USDT 수수료/실현손익/소스 SHA-256을 일관된 순서로 기록합니다. 금액은 `math/big.Rat`로 합산하고 12자리 초과 정밀도는 반올림 대신 차단하여 진입·청산 수수료 합계, 거래소 청산 실현손익, 펀딩 제외 잠정 순손익을 독립 교차 확인합니다.
- 펀딩 거래별 ID·시각·금액을 별도 계정 수준 증거로 추적하되 `model_funding_usdt: null`, `model_final_net_usdt: null`, `funding_allocated=false`입니다. **빈 펀딩 이력/합계 0도 모델 귀속이 0으로 입증됐다는 의미가 아닙니다.** 원래 증거 범위는 진입 첫 체결부터 청산 마지막 체결까지로 제한됩니다.
- `ValidateExternalModelFinancialEvidence`가 재직렬화·재해시된 결과를 포함해 수수료·펀딩 행의 중복·금액·총계·필수 차단 조건을 재검사합니다. `ValidateExternalModelFinancialAssertion`은 동일 기간의 미게시 후보금액만 수락하고 임의 펀딩 귀속, 모델 확정 순손익, 거래·정산 권한을 거부합니다. 이 계약은 실제 모델 소유권, 완전한 과거 계정 이력, Binance가 직접 서명한 기록이나 실정산 증명이 아닙니다.
- 기존 `cmd/demo-close-financial-review -model-evidence [-model-assertion <unposted.json>]` 및 `scripts/pc-demo-model-financial-audit.ps1` 추가. 종전 CLI의 기본 JSON 동작은 변경하지 않습니다. **Go 모의 LONG/SHORT·양/음/0 펀딩·변조·동시성·재시작 테스트를 추가**했습니다.
- 인터넷·계정·실주문·금융 COMMIT·원본 원장 변경 없음. 제품 HTTP 423 LOCKED 유지. `docs/DEV3126_MODEL_FEE_FUNDING_EVIDENCE_CONTRACT_KO.md` 참고.

## v0.4.11-dev.3.125 — 전역 예약·사건별 원자적 리허설 일관성 (HTTP 423 LOCKED)

- `InspectExternalGlobalAtomicConsistency`: 3.124 전역 등록부에 **정확히 예약된 배치**를 3.123 bounded signed 사건 감사 결과와 재대조. 비예약 배치, 일부 사건만 일치하는 배치, 다른 등록부 또는 변조된 기록을 차단.
- 각 사건의 linked event-scoped journal과 `intent/lifecycle/ownership/seal`을 독립 조회하고 봉인 상태 및 `InspectFinancialCommitPreflight`의 필수 BLOCKED 사전 조건을 확인. **모든 사건이 SEALED인 경우에만** `GLOBAL_ATOMIC_CANDIDATES_SEALED_NOT_POSTED`로 보고. 하나라도 미봉인인 경우 전체 배치의 후보 가시성은 false이며 봉인 지문도 숨김.
- 전역 등록부·각 사건 리허설·금융 사전검사·원본 증거를 반복 조회하여 도중 drift 감지. `cmd/demo-close-global-reservation -action consistency -manifest ...` 및 PowerShell `-Action consistency`는 완전 읽기 전용(ACK 불허), 어떠한 게시·실정산도 수행하지 않음.
- LONG/SHORT, 중단·재시작, 미봉인 배치, 예약 누락, 원자적 seal/registry 손상, 재시작 결과 동일성 회귀 테스트. **실제 전역 원자적 데이터베이스 COMMIT이 아님.** Binance 계정 동일성/전체 이력/펀딩 모델 귀속/독립 승인은 여전히 미확인; 제품 자동매매 HTTP 423 잠금, 실주문·금융 원장 불변. `docs/DEV3125_OFFLINE_GLOBAL_ATOMIC_CONSISTENCY_KO.md` 참고.

## v0.4.11-dev.3.124 — 전역 증거 예약 등록부·동시 충돌 방지 오프라인 리허설 (HTTP 423 LOCKED)

- `ExternalGlobalEvidenceReservationStore`: 기존 3.123의 원본 증거 검증 및 교차 충돌 검사를 통과한 사건 2~64개를 별도의 오프라인 등록부에 불변 sequence/O_EXCL+fsync 파일로 예약합니다. 배치별 감사 SHA-256, 사건별 주문·체결·펀딩 ID와 Hedge 방향 시간 구간, 이전 파일 checksum을 사슬로 연결합니다.
- 여러 번 예약할 때 이전 배치와 신규 배치를 합산해 충돌 검사하며 동일 배치 재요청은 멱등, 같은 clientOrderID·주문/체결/펀딩 ID·동일 Hedge 방향 보유 구간 중첩은 새 파일 없이 차단합니다. 미완성 파일·순번 누락·변조·symlink·승인 플래그 위조가 발견되면 재시작해도 자동 복구/삭제 없이 fail-closed 처리합니다.
- `cmd/demo-close-global-reservation`과 `scripts/pc-demo-global-reservation.ps1`의 기본 동작은 **inspect**이며, 별도 등록 의도와 `REVIEW_ONLY_NOT_SETTLEMENT` 확인이 있어야 원본과 분리된 등록부에만 쓰기 작업을 수행합니다. Original execution, ownership, fills, reservations, wallet, POST/DELETE, 정산 원장은 변경하지 않습니다.
- 모의 정상 LONG/SHORT, 새 배치 추가, 순서 독립·중복 재실행, 다중 goroutine 동시 실행, 타 배치 ID 충돌, 미완성/분실/변조된 순번, 소스 경로 보호 테스트. 각 결과는 실제 금융 예약/COMMIT 권한이 아니라 **오프라인 증거 ID 충돌 시뮬레이션**입니다.
- 계정 동일성·전체 거래 이력·실시간 private stream 연속성·펀딩 소유권 증명과 거래소 외부의 다중 프로세스 신뢰/PC E2E는 별도 과제입니다. 실제 금융 COMMIT, 자동 TP/SL 발행, 실제 오더, `HTTP 423` 해제는 미구현·미승인입니다. `docs/DEV3124_OFFLINE_GLOBAL_EVIDENCE_RESERVATION_KO.md` 참고.

## v0.4.11-dev.3.123 — 복수 사건 체결 ID 중복·Hedge 소유권 충돌 오프라인 감사 (HTTP 423 LOCKED)

- `InspectExternalCrossEventFinancials`: 서로 다른 정지된 데이터 스냅샷과 각 사건의 Package A incident JSON 2~64개를 먼저 **3.122 bounded financial inspector로 각각 재검증**한 뒤 체결 ID, 진입/외부 청산 주문 ID, 유한 시간 구간 내 펀딩 transaction ID 재사용 및 같은 Hedge 방향의 보유 기간 겹침을 결정적으로 검사합니다.
- 충돌은 `CROSS_EVENT_CONFLICTS_FOUND_NOT_POSTED`, 비충돌은 `CROSS_EVENT_NO_LOCAL_CONFLICT_OBSERVED_NOT_POSTED`로 표시하되 둘 다 결코 정산 승인/소유권 확정이 아닙니다. 오프라인 사본에 동일 계정이 연결됐다는 독립 증거가 없으므로 `shared_account_proven=false`, 전체 과거 이력·펀딩 귀속·실원장 COMMIT·거래 허가도 항상 false로 고정합니다.
- `cmd/demo-close-cross-event-audit -manifest ...`는 JSON manifest에 기록된 독립 사건을 읽기만 하고 충돌이면 종료코드 2를 반환합니다. source/read-only 원장 지문을 처음과 끝에 검증하고 새 파일·거래소 요청·주문 전송을 하지 않습니다. 단일 글로벌 proposal writer는 변경하지 않았습니다.
- 모의 LONG/SHORT, 이중 주문/체결/펀딩 ID, 같은 방향 시간 중첩, 서로 다른 방향 및 시점, 변조/원장 누락, 재시작·순서 독립성 테스트를 추가했습니다. Windows 실제 설치, Binance signed GET E2E, 실정산은 미검증. HTTP 423 LOCKED 및 원본 Lifecycle/Ownership 불변.

## v0.4.11-dev.3.122 — 외부 청산 정산 금액의 오프라인 범위 제한 검증 (HTTP 423 LOCKED)

- `InspectExternalCloseBoundedFinancials`: 기존 단일 사건 외부 청산 정산 제안과 보관된 Package A 진입·청산 signed GET 결과를 교차 검증. 원본 실행 증거 SHA-256 재조회, 주문/체결 ID, LONG/SHORT 수량, USDT 수수료, 거래소 실현손익, 원본 제안의 펀딩 제외 순손익을 정수 비율 연산으로 대조하며 변경·손상·다른 사건을 차단.
- signed 조회 범위의 펀딩 순액은 **미배분 계정 총액**으로만 반환하고 모델별 손익에 가산하지 않음. 현시점 signed 요청 재조회, 전체 과거 이력·소유권·펀딩 귀속 완전성은 입증하지 않음.
- `cmd/demo-close-financial-review` 오프라인 읽기 전용 CLI와 LONG/SHORT/음·양 펀딩/체결 이력·원본 변조·재시작 회귀 테스트 추가.
- 실제 금융 COMMIT, 제품 자동매매/TP·SL 발행·취소, 거래 API 락 해제 **없음**. `HTTP 423 LOCKED`, 원본 Lifecycle·Ownership·지갑 원장 수정 없음. 자세한 내용은 `docs/DEV3122_OFFLINE_FINANCIAL_AMOUNT_RECONCILIATION_KO.md`.

## v0.4.11-dev.3.121 — Binance Demo 보호주문 signed PC 진단 보고서 (LOCKED)

- 3.120에서 구현한 `InspectProtectionExecutionApprovalGate`를 기존 `cmd/demo-signed-inspect`에 `-protection-client` / `-protection-window-hours` 전용 모드로 연결. 실제 Binance Futures Demo signed **GET** 두 번(모의 테스트 완료, 실제 PC 조회 미실행), 저장된 예약·체결 Lifecycle·보호계획·로컬 원장을 대조한다.
- 지갑 이벤트 해시 체인에서 기존 계정 지문을 **읽기 전용**으로 확인하며, 이벤트가 없는 경우, 다른 계정 키, 서명 조회 오류, 조회 중 지갑·예약 변경을 차단. API 키/secret은 결과 파일에 기록하지 않는다.
- `pc-results/`에 O_EXCL 생성 전용 JSON 감사 보고서, BLOCKED 오류 코드, 무결성 SHA-256 저장. 재확인은 별도 JSON으로만 수행하며 자동 재전송/정산/락 해제 없음.
- `scripts/pc-demo-protection-signed-audit.ps1` 추가. `HTTP 423 LOCKED` 및 모든 실제 보호주문 발행/취소/금융 정산 비활성 유지. 상세 문서: `docs/DEV3121_PC_DEMO_PROTECTION_SIGNED_AUDIT_KO.md`.

## v0.4.11-dev.3.120 — independent signed account-wide protection approval REVIEW gate (HTTP 423 LOCKED)

- Add `InspectProtectionExecutionApprovalGate`, a **read-only** gate accepting a GET-only authenticated Demo recovery reader and an out-of-band expected API-key fingerprint. Requires the exact on-disk original immutable reservation, a signed execution lifecycle at FILLS_IMPORTED, a matching entry journal, and a sole PREPARED TP/SL plan; independently replays original order/fill ownership via `ReviewRecovery`, checks bounded recent signed orders/trades/income, and rejects unmatched external activity or private-stream discontinuity markers.
- Bracket ownership reconciliation with two independent fresh ALL-symbols account/conditional/normal-order and bounded-history signed REST baselines; require non-overlap, identical canonical exposure hashes, stable local four-directory evidence pin and unchanged protection/cancel inventory. Reject other symbols, foreign collateral, multiple owned Hedge positions on either side, unexpected Algo/orders, non-isolated Hedge exposure, tampering, stale data and signed failures.
- The only positive classification is `SIGNED_BOUNDED_OWNER_MATCH_NOT_APPROVED`; submission, sibling cancellation, trading and settlement authorization flags always false. No operator-boolean override, no live POST/DELETE, no server/daemon integration. Bounded REST cannot attest expired lifetime history, stream continuity, model funding or independent atomic OCO.
- Add LONG/SHORT, restart, API-key fingerprint mismatch, signed drift/failure, unowned trade/order, and local evidence mutation tests. Real Binance Demo E2E and Windows PowerShell install remain unverified. Product HTTP 423 / runtime `ErrProtectionApproval` retained.

## v0.4.11-dev.3.119 — sealed offline TP/SL submit/cancel + no-replay recovery rehearsal (HTTP 423 LOCKED)

- Introduce `RehearseEntryProtectionDispatch` behind a **package-private sealed simulated-transport marker**. Reuse existing one-shot TP→SL `ArmDemoProtection`, signed GET-only `RecoverDemoProtectionArming`, `ReviewRecoveredEntryProtectionLifecycleSigned`, and one-shot supervised sibling `SuperviseSiblingCancelOnce`. No production `exchange.Client` can satisfy the private interface; not connected to HTTP or unattended trading loop.
- Fix 3.118 inspection gap after a lost protection POST ACK: `ReviewRecoveredEntryProtectionLifecycleSigned` independently signed-GETs a single open Algo matching a `MAY_HAVE_SENT` leg's immutable plan before allowing GET-only recovery; rejects unknown/duplicated/modified Algo identities and never retries POST.
- On cancel: require fresh triggered+active signed lifecycle with exact exit fills, no previously dispatched DELETE, and strictly matching mock ownership evidence; after a DELETE `MAY_HAVE_SENT`, later runs do signed GET only, even if the sibling stays active or is already canceled. LONG/SHORT, TP ACK loss, SL ACK loss, DELETE ACK loss, restart, corrupted/foreign evidence and signed failure tests.
- Disable the potentially unsafe direct production-runtime convenience methods `DemoProtectionRuntimeCoordinator.Arm` and `.CancelTriggeredSibling` (they previously accepted a caller's evidence as a write gate) until independently audited signed ownership and a supported approval flow exist; both return `ErrProtectionApproval`.
- **No production POST/DELETE, no Binance Demo E2E, no financial settlement, no live reconciliation authorization, no auto trading unlock. HTTP 423 LOCKED.** This release is a local offline mock rehearsal, not an operator order tool.

## v0.4.11-dev.3.118 — signed-only TP/SL lifecycle continuity, sibling cancel review (HTTP 423 LOCKED)

- `ReviewRecoveredEntryProtectionLifecycleSigned`: bridges 3.117 `FILLS_IMPORTED` + deterministic TP/SL group to persistent arming checkpoints, separately authenticated signed Algo GETs and signed exit-trade reconstruction. Performs two independent signed full-account preflight reads around inspection and rejects drift, unmanaged conditional/normal orders, foreign stores, damaged/colliding markers, conflicting cancel intent, missing or altered signed data.
- Classifies prepared/unsent, TP or SL MAY_HAVE_SENT (no replay), one-leg observed, two-leg active (NOT atomic OCO), single trigger with signed exit fills (operator cancel review only), attempted sibling DELETE (no replay), full triggered+canceled pair (NOT financially settled), partial exit, double trigger or HALT.
- No POST/DELETE transport method in the input interface, no automatic arming/canceling/retry or trading authorization; never writes financial/Ownership ledgers. Also serialize the existing public-market signal sampler: prevent an overlapping successful background tick from overwriting a newer disconnected status (new concurrency regression test). Long/short, crash/restart, partial/double, exchange-GET failure, time drift, wrong identity/cancel target and data tampering tests. Real account history/funding/financial COMMIT, Binance Demo E2E, Windows native test remain unverified. HTTP 423 LOCKED.

## v0.4.11-dev.3.117 — supervised signed entry recovery → local TP/SL plan bridge (HTTP 423 LOCKED)

- `RecoverAndPrepareEntryProtectionSupervised`: signed-only `MAY_HAVE_SENT`/`SIGNED_OBSERVED` entry fill recovery → full signed order/position ownership audit → exact `FILLED` quantity and exclusive Hedge-side owner check → deterministic `entry_<clientOrderID>` TP/SL preview and local durable `Prepare` → second fresh signed account/continuity check. No exchange order submission, cancel, auto retry, financial settlement, or trading authorization.
- GET-only `EntryProtectionSignedPreflightReader` interface prevents an accidental Algo POST through the bridge. Both TP and SL remain `PREPARED_NEVER_DISPATCHED`, never armed by this code. A separately supervised arming function already exists, but is not called here or by product runtime.
- LONG/SHORT, ACK-loss, restart/idempotency, NEW/partial/terminal-incomplete, externally closed side, missing signed GET, stale quote, foreign store, and concurrent signed change mock tests. No real Binance Demo trade or Windows E2E was performed.
- Cumulative release scripts/version guards/manifest updated. Real financial COMMIT, protection trigger/cancel orchestration, full funding/external-trade reconciliation, and unattended multi-model auto trading remain NOT ready. HTTP 423 LOCKED retained.

## v0.4.11-dev.3.116 — Codex R-07/R-08/R-09 deployment safety (HTTP 423 LOCKED)

- Mandatory out-of-band `ExpectedSHA256` before `pc-reviewed-update.ps1` may extract/execute any bundled updater. Validate copied ZIP, not just ZIP-authored manifest.
- Retire `pc-tonight-demo.ps1` opt-in unprotected Demo order path; retain public WS read-only market check, bump version guard.
- Fix `/api/v1/bootstrap.phase` stale numeric 3 using `version.PhaseMajor=4`; keep version API string phase and `phase_name` response compatibility.
- Add regression tests and annotate historical PC instructions as obsolete. No new Binance orders, financial posting or trading unlock; Windows PowerShell 5.1 E2E remains to be verified on PC.

## v0.4.11-dev.3.115 — 출처 분리형 공통 원장 읽기 포트 (HTTP 423 LOCKED)

- `ExternalLedgerReadPort` 및 `ReadExternalLedgerSnapshot` 추가: REAL 실행 증거와 미적용 SHADOW 제안을 **명시적으로 분리**하고 출처에 맞는 SHA-256만 받도록 강제. SHADOW는 SEAL 외에 게시 체크포인트 지문을 별도로 고정. 오래된·혼합·미게시 증거는 오류 차단.
- `ReadSeparatedRealAndShadow`의 실원장·SHADOW 읽기를 공통 포트를 통한 검증으로 연결. 오프라인 CLI `demo-ledger-consistency -action source -source real|shadow` 추가. 기존 CLI의 `baseline`/`bundle`/`compare` 응답 계약 유지.
- LONG/SHORT·재시작·출처별 지문 혼용·변조·미게시·동시 읽기 회귀 테스트 추가. PowerShell 릴리스 버전 가드 갱신.
- **제한:** 포트는 읽기 전용이며 라이브 금융 원장 전체의 원자적 커밋 번호를 제공하지 않음. SHADOW PUBLISHED도 금융 정산이 아님. HTTP 423 잠금, 금융 COMMIT 미구현, 전체 signed 이력·펀딩·Ownership 확정 및 PC Binance Demo E2E 미완료. `docs/DEV3115_COMMON_LEDGER_READ_PORT_KO.md` 참고.

## v0.4.11-dev.3.114 — 원본 Execution 지문 고정된 Ownership/Lifecycle 통합 조회 (HTTP 423 LOCKED)

- `ReadPinnedRealLedgerBundle`: caller가 이전 `baseline`에서 받은 `real_evidence_sha256`를 필수 입력으로 받아 실제 Ownership 및 Lifecycle 감사를 **동일한 네 디렉터리 지문**으로 고정해 반환. 중간 파일 변경, 오래된 지문, 취소 컨텍스트, 다른 clientOrderID 또는 서로 다른 감사 상태는 오류로 중단. 결과는 완전한 체결/정산을 의미하지 않으며 승인 플래그는 항상 false.
- `ExternalShadowCommitStore.ReadSeparatedRealAndShadow`에서 기존 실원장 관측을 해당 공통 핀 조회로 대체. Shadow 후보의 게시/봉인 지문과 실원장 지문은 **서로 분리** 유지. 단계별 외부 결제·PnL 조회기는 변경되지 않음.
- 오프라인 CLI `cmd/demo-ledger-consistency -action bundle -real-pin <baseline SHA-256>` 지원. LONG/SHORT, 재시작, 파일 변조, 중간 변경, 컨텍스트 취소, 로컬 진입 체결 누락 테스트 추가. PowerShell 버전 가드 업데이트.
- **범위 제한:** 해당 지문은 데이터 4개 디렉터리의 로컬 파일 관측 지문이며, 실제 커밋 시퀀스 번호나 전역 트랜잭션이 아니다. Writers를 정지한 데이터 복사본에서만 사용할 것. 전체 금융 원장 COMMIT, 펀딩·전체 Binance signed 이력·Ownership 귀속, 제품 자동 보호주문·다중 모델 주문, 실제 PC Binance E2E는 미완료. HTTP 423 잠금 유지.

## v0.4.11-dev.3.113 — 실제 원장 관측과 SHADOW 후보의 분리·지문 대조 (HTTP 423 LOCKED)

- `InspectRealLedgerObservation`: 실제 `requests`·`fills` 원본과 `execution_lifecycle`를 **읽기 전용**으로 감사하고 네 원본 디렉터리의 SHA-256 지문을 반환합니다. 새 원장 파일이나 Binance 주문·API 요청을 생성하지 않습니다.
- `ExternalShadowCommitStore.ReadSeparatedRealAndShadow`: 호출자가 고정한 **실제 원장 지문**과 **SHADOW SEAL 지문**을 모두 요구합니다. SHADOW `PUBLISHED` 상태라도 실제 원장 적용으로 해석하지 않고, 두 원장 관측값을 조회 전후 재검증합니다. 중간 변경·변조·오래된 지문·잘못된 식별자는 실패 차단됩니다.
- `cmd/demo-ledger-consistency`: `baseline`(실제 원장 SHA-256 조회) 및 `compare`(실제 원장과 SHADOW 후보를 구분해 출력)용 오프라인 전용 CLI 추가. LONG/SHORT, 미게시, 변조, 조회 중 변경·재시작 테스트 및 PC 스크립트 버전 가드 갱신.
- **미완료:** 기존 실제 원장 조회기들의 단일 COMMIT 프로토콜로의 전환, 실제 금융 COMMIT, 완전한 signed 계정/펀딩/소유권 입증, 제품 자동매매/보호주문 연결과 실환경 E2E. 이번 결과는 *비원자적 파일 관측을 감시하는 독립 감사 기능*이지 동시 쓰기 시스템의 트랜잭션 보장이 아닙니다. HTTP 423 유지.

## v0.4.11-dev.3.112 — SHADOW 금융 원장 쌍 단일 게시 경계 (HTTP 423 LOCKED)

- `ExternalShadowCommitStore`: 사건별 독립 SHADOW 저장소에 두 후보를 `PREPARED`로 동시 준비하고 별도 `PUBLISHED` 체크포인트가 존재할 때만 쌍으로 조회합니다. 다른 사건·변조·부분 파일·중간 종료·재시작·중복 호출을 fail-closed로 점검합니다.
- 기존 `InspectFinancialCommitPreflight`의 **차단** 결과를 검증 근거로만 사용합니다. `PUBLISHED`는 실제 금융 COMMIT이 아니며 `posting_applied`, `settlement_authorized`, `trading_authorized` 모두 false입니다. 제품 원장·서버·Demo 주문 경로를 변경하지 않았습니다.
- 모의 LONG/SHORT, 손상·누락, 동시 재실행 회귀 테스트 및 릴리스 버전·PowerShell 가드 갱신.

## v0.4.11-dev.3.111 — 사건별 금융 COMMIT 사전 차단 검사 및 PC 버전 회귀 수정 (HTTP 423 LOCKED)

- `InspectFinancialCommitPreflight` 추가: 3.110 사건별 봉인 저널, Lifecycle·Ownership 미적용 후보, 증거 매트릭스를 같은 pinned seal SHA-256에 연결하여 읽기 전용으로 재검증합니다. 전체 signed 계정·거래 이력·펀딩·소유권·원자적 게시·독립 승인 증거가 없으면 *항상* `FINANCIAL_COMMIT_BLOCKED_INDEPENDENT_EVIDENCE_REQUIRED`를 반환합니다. 금융 원본에 쓰거나 거래 권한을 부여하지 않습니다.
- `cmd/demo-close-atomic-rehearsal -linked-event -action commit-gate -commit <sha>` 및 PowerShell `-LinkedEvent -Action commit-gate -CommitSHA256` 지원. 봉인 이전·다른 사건·해시 변조·원본 변경은 오류로 차단. LONG/SHORT·재시작·동시 조회 회귀 테스트.
- 과거 3.100/3.109에 묶여 실행되지 않던 12개 PC 정산/감사 스크립트의 버전 가드를 3.111로 정리하고 Go 회귀 테스트로 고정. 스크립트 자체의 Windows 실행 및 실제 데이터 검증은 아직 수행하지 않았습니다.
- **미완료:** 실제 Lifecycle·Ownership·PnL 금융 COMMIT, 완전한 signed 거래 이력·펀딩·귀속 검증, 제품 보호주문·다중 모델 실행, 전체 PC Binance Demo E2E. HTTP 423 잠금 유지. `docs/DEV3111_FINANCIAL_COMMIT_PREFLIGHT_KO.md` 참고.

## v0.4.11-dev.3.110 — 사건별 정산·원자성 후보 증거 연결 (HTTP 423 LOCKED)

- `NewExternalAtomicLinkedEventStore` 추가: 동일 `clientOrderID`의 **3.109 사건별 FENCED 정산 저널**에서만 3.108 사건별 리허설 후보를 생성하는 명시적 opt-in 경로. 기존 3.108 사건 저장소·단일 사건 저널은 변경 없이 유지.
- `InspectScopedSettlementContinuity`는 사건 ID, 모델·Hedge 방향, 원본 증거 SHA-256, 저널 PREPARED/FENCED SHA-256, 이중 장부 후보와 봉인된 커밋 지문을 함께 검증하고 재시작 시 변조·누락·변경을 거부. `-linked-event -action continuity -commit <sealSHA>` 및 PC의 `-LinkedEvent -Action continuity`로 **읽기 전용** 조회 가능.
- LONG/SHORT, 중단된 봉인, 불일치 지문, 변조, 레거시 혼합 차단 테스트. HTTP 423 주문 잠금, 원본 Lifecycle/Ownership, 정산 및 자산은 변경 없음. 이 결과는 **금융 COMMIT·완전한 Binance signed 이력·펀딩 귀속·독립 승인**이 아니며, 선행 상위 저장소의 다중 사건 통합도 아직 미완료.

## v0.4.11-dev.3.109 — event-scoped NON-POSTED settlement journal (HTTP 423 LOCKED)

- `NewExternalSettlementEventJournalStore(dataDir, clientOrderID)` stores `prepared.json → fenced.json` under separate `data/trading/external_close_settlement_journal_events/<clientOrderID>/` directories. Checkpoint creation remains append-only and never mutates financial ledgers.
- Revalidates original evidence and client IDs, rejects legacy same-event collisions, orphan/torn/symlink/extra records, and refuses silent migration. Legacy checkpoint inspection remains explicitly read-only in the CLI/PowerShell (`-legacy-read-only` / `-LegacyReadOnly`).
- Added LONG/SHORT, restart, evidence-drift, legacy collision, tamper, simultaneous store instance and subprocess write tests. New journal may coexist with a different historical event without modifying it.
- **NOT COMPLETE:** upstream dual-ledger/ownership proof paths are not yet fully event-scoped, and the atomic rehearsal reader still depends on the historical journal. Real signed full-account history, funding attribution, exact per-owner settlement, and financial COMMIT remain incomplete. No Binance network or product order path change; HTTP 423 preserved. See `docs/DEV3109_SETTLEMENT_JOURNAL_EVENT_SCOPING_KO.md`.

## v0.4.11-dev.3.108 — event-scoped NON-POSTED rehearsal checkpoint isolation (HTTP 423 LOCKED)

- New `NewExternalAtomicEventStore(dataDir, clientOrderID)` isolates append-only `intent → lifecycle → ownership → seal` records under `data/trading/external_close_atomic_events/<clientOrderID>/`; read-only construction and inspection create no directories.
- The event store binds the client ID and revalidates source/journal checksums on every stage and candidate read; rejects foreign IDs, unsafe parent entries/symlinks, malformed or out-of-order records, and inconsistent source revisions. Same-event legacy checkpoints cannot be copied or replayed into the new location silently.
- Existing single-directory `NewExternalAtomicRehearsalStore` remains an **explicit read-only** legacy access path via `-legacy-read-only` / `-LegacyReadOnly`. Default CLI/PowerShell mode is new per-event storage. Legacy writes are unavailable through the command. No automatic migration.
- Added LONG/SHORT event restart and seal-token tests, parent/event corruption checks, legacy identity collision tests, same-event parallel independent-instance and subprocess file collision tests (the losing writer can fail closed).
- **No financial posting**: all records and pair views remain non-posted. No original Lifecycle, Ownership, PnL, positions, fills or fee/funding journals change. There is no account signed-history completeness proof, real commit boundary or real Binance order execution. Product START/STOP/manual order HTTP 423 remains unchanged. Do not use this for live automated trading.

## v0.4.11-dev.3.107 — commit-token pinned cross-ledger candidate read (HTTP 423 LOCKED)

- `ExternalAtomicRehearsalStore.Inspect` adds `commit_sha256` only for an authenticated, complete simulation `seal.json`; no token is visible before the final checkpoint.
- `ReadLedgerPair`, `ReadLifecycleCandidate`, `ReadOwnershipCandidate` expose the two **NON-POSTED** proposed updates under the exact same caller-pinned seal checksum. Each read independently revalidates upstream fenced sources and the entire immutable checkpoint chain; missing/stale token, tampering, source drift and partial writes fail closed.
- Added offline `-action pair -commit <seal SHA-256>` to the existing CLI and `-Action pair -CommitSHA256 ...` to the PowerShell script; reads only, no Binance network or ledger writes. Added LONG/SHORT, tamper, source drift, restart, and token mismatch tests.
- **NOT an actual financial commit**: the production lifecycle/ownership/accounting readers do not use this projection. Signed complete history, funding, explicit independent ownership attestation and operator settlement approval are still missing. The rehearsal directory is currently single-case, not multi-event. No changes to real journals or HTTP 423 locks. See `docs/DEV3107_ATOMIC_PAIR_READ_KO.md`.

## v0.4.11-dev.3.106 — crash/restart cross-ledger atomicity rehearsal (HTTP 423 LOCKED)

- `ExternalAtomicRehearsalStore`: fenced NON-POSTED external-settlement journal and verified dual-ledger evidence are revalidated on every read/write. Immutable `intent → lifecycle → ownership → seal` checkpoint chain models a crash-recoverable two-ledger transition. Only a valid final `seal.json` reveals both *proposed* views as one joint read model.
- All checkpoint writes use exclusive creation and durable file/directory sync. Restart reuses validated prefixes idempotently; torn/orphan/unknown/symlink/forged-authority records and source evidence drift fail closed without resetting the checkpoints.
- Added offline-only `cmd/demo-close-atomic-rehearsal` and `scripts/pc-demo-atomic-rehearsal.ps1`; write actions require explicit `SHADOW_ONLY_NO_FINANCIAL_POSTING` confirmation. LONG/SHORT, concurrency, restart at each boundary, tamper and missing-prerequisite regression tests.
- **NOT a financial posting engine:** does not mutate lifecycle, fills, ownership, reservations or PnL. The existing product readers do NOT participate in a common commit protocol, and independent signed full account history, model funding attribution and settlement authorization are still missing. This simulated seal MUST NOT be interpreted as `SETTLED` or trading permission. HTTP 423 remains locked. See `docs/DEV3106_ATOMIC_REHEARSAL_KO.md`.

## v0.4.11-dev.3.105 — read-only exit / ownership exposure correlation (LOCKED)

- Add `ReviewExitOwnerExposure` and `InspectSignedExitOwnerExposure`: compare immutable protection ownership plan and verified signed TP/SL fill review against a bounded account-wide Demo REST observation and caller-supplied, independently attested pre-exit ownership snapshot.
- Fail closed for missing/duplicate/mismatched owner, fill quantity mismatch, invalid or stale baseline, open orders, ambiguous terminal states and conflicting leg quantities.
- Classify same-Hedge-side multi-owner exposure as `POOLED_SIDE_AMBIGUOUS_NOT_SETTLED`, even if observed aggregate quantity matches a projected balance. Single-owner observed Flat also remains `NOT_SETTLED` because account snapshots cannot prove complete trade history, funding attribution or lack of intervening closes.
- Add LONG/SHORT-compatible mock-signed and failure tests. No original ledger writes, settlement posting, product HTTP integration, exchange orders or cancellations. HTTP 423 remains LOCKED.
- Release is an internal safety checkpoint; no live/PC Binance E2E validation performed.

## v0.4.11-dev.3.104 — signed TP/SL exit fill reconciliation (LOCKED)

- Added `InspectSignedExitFills`: independent read-only double signed GET + signed actual-order trade lookup for both TP/SL legs, exact rational comparison of `actualQty` and all fills, global trade-ID duplicate detection, order-ID isolation, changed-observation detection, and aggregate owner-quantity overfill prevention.
- Classifies matching-but-unsettled, partial, open-leg, and double-trigger exits without claiming Ownership, pooled account balance, funding attribution, or financial settlement. No ledger mutations, order POST/DELETE or server integration.
- Added targeted normal, partial, double-trigger, overfill, missing/duplicate fills, wrong-side identity, and changed-state tests. HTTP 423 trading lock retained.

## v0.4.11-dev.3.103 — supervised sibling cancel mock integration (HTTP 423 LOCKED)

- `SuperviseSiblingCancelOnce` integrates immutable TP/SL plan, fresh signed GET for **both** legs, persisted single-target cancel intent, one-shot `MAY_HAVE_SENT` dispatch and signed-only restart recovery.
- Mock tests: terminal cancel once, ambiguous DELETE acknowledgement, restart without DELETE, sibling later FINISHED requiring reconciliation, invalid remote identity, and simultaneous trigger fail-closed.
- **INTERNAL ONLY / NOT A LIVE GATE:** caller-supplied ownership proof is not independently attested; the product server and automatic trading loop remain unconnected. No user account access, actual DELETE, settlement or trading authorization. Existing HTTP 423 lock unchanged.

## v0.4.11-dev.3.102 — TP/SL sibling cancellation crash continuity (LOCKED)

- Reject a protection pair whose independently observed TP and SL exchange Algo IDs are equal; such evidence must never select a cancellation target.
- Added `InspectSiblingCancelContinuity` (local/read-only): validate immutable protection plan, fresh independently sourced TP/SL evidence and complete ownership-audit markers, correlate the exact sibling Algo/client ID to the immutable one-shot cancel intent, and classify absent intent / intent-only / MAY_HAVE_SENT / signed-observed terminal state without retrying DELETE.
- LONG regression tests cover duplicate Algo IDs, swapped cancellation target, corrupted durable intent, stale signed evidence, double-trigger, absent independent account/ownership proof, and crash/restart states. The function does not fetch signed GETs itself and is not a substitute for signed-account provenance verification.
- No Binance network access, POST/DELETE, automatic cancellation, financial posting, product-server wiring or trading authorization added. Original HTTP 423 locks remain.

## v0.4.11-dev.3.101 — 감독형 진입 체결 → 보호주문 내부 실행 경로 연결 (LOCKED)

- `ArmImportedEntryProtectionSupervised`: 기존 `FILLS_IMPORTED` 진입 예약·Lifecycle 전체 해시 체인·진입/소유 수량·보호 그룹 체크포인트와 최신 Demo signed 계정 증거를 검증한 뒤 기존 `ArmDemoProtection`의 TP/SL 단발 실행기를 호출합니다.
- LONG/SHORT 모의 거래소에서 TP/SL 두 레그 정상 제출, 재호출 중복 방지, 변조·미수입 체결·잘못된 수량, 두 번째 레그 ACK 불확실성에 따른 재전송 금지를 검증했습니다.
- 이 내부 함수는 제품 HTTP·자동매매 실행 루프에는 **연결되지 않았으며**, 실제 Binance 주문을 호출하지 않았습니다. 호출 자체는 Demo 주문 POST 기능이므로 운영 승인·전체 거래 이력·펀딩·정산·계정 소유권 검증이 없는 환경에서 사용하면 안 됩니다.
- 외부 청산 원자적 정산, 한쪽 보호주문 체결 후 형제 주문 자동 정리, 모델 병렬 자동주문 및 실환경 E2E는 미완료입니다. 제품 START·STOP·수동 주문은 계속 HTTP 423 LOCKED입니다.

## v0.4.11-dev.3.100 — 2026-11-04 — entry/protection evidence mismatch hardening (LOCKED)

- `InspectEntryProtectionContinuityGroup` now compares the exact reservation Intent plan and digest with the Lifecycle root, instead of relying solely on a self-consistent hash chain.
- Requires the imported FILLED executed quantity to equal both the reservation's requested quantity and the owner quantity using exact rational decimals, preventing mismatched entry evidence from being paired with a signed pooled-side snapshot.
- Added LONG/SHORT regressions for rehashed wrong quantities, resealed swapped Lifecycle plan, and rewritten reservation intent digests. No live orders, no retry/cancel, no financial settlement and no auto-entry→TP/SL production connector enabled.
- Fixed a stale parent-version assertion in `internal/version/version_test.go` observed in the prior source. Preserved the product HTTP 423 lock.

## v0.4.11-dev.3.99 — 2026-11-03 — read-only entry/protection continuity + mock recovery integration (LOCKED)

- Added `InspectEntryProtectionContinuityGroup`: checks immutable entry reservation, full imported-fill lifecycle hash chain, durable protection plan, owner identity/quantity, TP and SL durable checkpoints, and a fresh signed Demo account view in one fail-closed read-only report.
- Added mock exchange integration regressions for LONG/SHORT TP/SL checkpoint transitions, ambiguous ACK/second-leg timeout, restart/reinspection without any new POST, stale or absent signed data, unexpected Algo order, and tampered entry lifecycle.
- The new report never authorizes order submission, retry, cancellation, settlement or trading. It is NOT wired into a live auto entry→TP/SL runtime and does NOT resolve atomic external close settlement. Demo HTTP 423 LOCKED remains enforced.
- Validation: targeted protection suite, go vet, Windows amd64 cross-build. Whole-project test suite not claimed as passed.

## v0.4.11-dev.3.98 — 2026-11-03 — entry-to-protection lifecycle chain validation (LOCKED)

- Hardened `ReviewEntryProtectionHandoff` to validate all lifecycle stages, ordered sequences, event checksums, previous hashes, timestamps, and per-stage evidence requirements instead of trusting only the final `FILLS_IMPORTED` event.
- Rebuilt existing handoff test fixture as a valid RESERVED → MAY_HAVE_SENT → SIGNED_OBSERVED → FILLS_IMPORTED chain; added missing/tampered/reordered/rehashed evidence regression cases.
- Advanced version-gated PC scripts and internal release identifiers. Preserved Demo trading HTTP 423 LOCKED; no live TP/SL submission, external settlement, or real account mutation enabled.
- This is a safety checkpoint, NOT a completed automated entry-to-protection execution integration.

## v0.4.11-dev.3.97 — 2026-11-03 — restart checkpoint classification and Algo-ID separation (LOCKED)

- Added `InspectProtectionArmingCheckpoint`, a read-only durable checkpoint classification for PREPARED / TP MAY_HAVE_SENT / TP only observed / SL MAY_HAVE_SENT / both legs previously observed. Reports never authorize POST, retry, cancellation or trading; old signed observations cannot establish that the legs remain active.
- Fail closed when the two independently planned TP/SL legs reuse the same exchange Algo ID in persisted observations, after an arming readback or at recovery completion. Both LONG/SHORT checkpoint tests, orphan marker tests and forged duplicate durable Algo-ID regression tests added.
- No product-server protection dispatch, automatic sibling cancellation, atomic external-close settlement or demo live orders added. HTTP 423 trading lock remains enforced. Whole-account restart reconciliation remains a separate unfinished prerequisite.

## v0.4.11-dev.3.96 — 2026-11-03 — protection preflight hardening (LOCKED)

- Rejects nil open-order/algo lists, invalid bracket chronology, missing USDT assets, corrupt portfolio fields before attempting internal TP/SL arming or restart readback.
- Rechecks cancelled context after durable TP MAY_HAVE_SENT and before the first POST; never retries ambiguous legs.
- Adds signed-bracket failure injection and cancellation regression tests. Does **not** connect automatic TP/SL submission to product server; HTTP 423 remains in place.

## v0.4.11-dev.3.95 — 2026-11-03 — Read-only entry-to-protection handoff (LOCKED)

- Added a pure fail-closed handoff shape review between owned entry `FILLS_IMPORTED`, a durable owner-sized TP/SL plan, and a fresh Demo signed account bracket.
- Rejects missing/altered reservation checksums, wrong owner or side, incomplete fill data, stale/missing account snapshot, and unexpected existing Algo orders; LONG/SHORT tests cover all cases.
- This is NOT exchange-order orchestration, a validation of the full lifecycle hash chain, an OCO implementation, a proof of signed provenance or trading authorization. Product HTTP 423 remains enforced.

## v0.4.11-dev.3.94 — 2026-11-03 — Offline settlement evidence matrix (LOCKED)

- Added a read-only cross-evidence inspection that revalidates immutable proposal, posting gate, dual-ledger and restart journal, compares entry/exit trade identities, and distinguishes locally supported facts from independently unverifiable settlement prerequisites.
- CLI `demo-close-settlement-journal -action inspect-evidence` and PowerShell wrapper `-Action inspect-evidence`. No POST, network call, original ledger mutation, operator approval or trading permission.
- Added LONG/SHORT, restart stage, unauthorized flag forgery and source-tamper regression tests. Financial atomic posting, external close ownership attribution and TP/SL live path remain UNIMPLEMENTED.

## v0.4.11-dev.3.93 — 2026-11-03
- 기존 외부 청산 정산 저널에 읽기 전용 재시작 복구 검사(없음/PREPARED/FENCED)를 추가하고 CLI·PowerShell로 노출.
- 끊긴 체크포인트·원본 변경·부적절한 추가 파일은 fail-closed. 원본 장부 적용·주문 허가는 유지 차단.
- `docs/DEV393_SETTLEMENT_RESTART_INSPECTION_KO.md` 및 회귀 테스트 추가.

## v0.4.11-dev.3.92 — 2026-11-03 — explicitly scoped owned-entry fill recovery (LOCKED)

- Added `ExecutionLifecycleStore.RecoverOwnedEntryFillsOnly`: reuse the existing independently signed two-order-GET + userTrades reconciliation and immutable `OwnershipStore.ImportDemoOrder`, stopping at `FILLS_IMPORTED` rather than proceeding into whole-side reconciliation or final settlement. This allows a verified entry fill gap to be locally repaired in a future separately operator-approved workflow without treating the external/manual close as an owned close.
- No new POST method, HTTP endpoint, PowerShell import action or automatic invocation. This is an internal library capability with local ledger mutation **only if explicitly invoked by future audited code**. It was not run on user data and must NOT be invoked against the incident before fresh evidence, backups, operator authorization and independent audit.
- Both LONG and SHORT: tested lost ACK, duplicate/restart, missing signed order fail-closed, rejected exit action, and crash after persisted signed import but before lifecycle commit. No duplicate exchange submission, no reservation finalization, no trading authorization.
- **Still blocked:** original external close ownership, full account history and funding attribution, atomic cross-ledger financial posting, TP/SL live protection and full multi-model live execution. HTTP 423 unchanged. This is **not** completed financial settlement.

## v0.4.11-dev.3.91 — 2026-11-02
- Internal server factory now selects existing shared multi-model runtime when multiple registered model IDs are selected; no product start or orders enabled.
- Validates duplicate/unknown IDs before constructing runtime; preserves caller's model selection slice; exposes read-only `internal_multi_factory_wired` evidence.
- Added focused route-selection and locked-endpoint regression tests; all settlement/TP-SL/unsafe execution blockers still open.
- Includes non-authorizing Codex follow-up review brief and audit progress notes.

## v0.4.11-dev.3.90 — 2026-11-02 — fresh signed entry recovery review and UI escaping (LOCKED)

- `RecheckPackageAEntryRecovery`: verify the original immutable entry archive and recovery plan against current local journals; perform two fresh signed Demo order GETs bracketing the exact signed userTrades GET, and compare every original gap fact and digest. Reject stale plans, modified trades/fees, absent or contradictory lifecycle evidence and any source drift. Return SHA-256 sealed NON-POSTED report with last durable lifecycle boundary (or explicit absence) and unresolved blockers; report does NOT assert exact accident root cause.
- `demo-signed-inspect -package-a-action recheck-entry-recovery` and `scripts/pc-demo-package-a.ps1 -Action recheck-entry-recovery` provide **operator-invoked, GET-only** review of an existing entry archive from `pc-results`. Requires server stopped and Demo credentials in the existing installation, but never submits/cancels an order or imports/edits a fill/lifecycle/ownership record. It writes only a new, distinct report in `pc-results`. Not run on real Binance by this release build.
- Fixed `operationsOverviewBanner` HTML injection path: construct `<b>` and `<span>` with DOM APIs and assign *all* dynamic server/error text using `textContent`; tested hostile HTML payload as plain text.
- LONG/SHORT, missing/actual `MAY_HAVE_SENT`, source mutation, signed data drift, truncated/malformed inputs, credential fail-closed and CLI unsafe filename regression tests.
- **Still not implemented:** atomic external-close financial posting, owner/model funding attribution, automatic TP/SL and live multi-model products. The current HTTP order APIs remain 423 LOCKED, every signed recovery result has trading/settlement authorization false. PC tests and exact incident root cause are pending.

## v0.4.11-dev.3.89 — 2026-10-29 — persisted zero-funding archive fingerprint fix (LOCKED)

- Fix false-positive `signed GET detail fingerprint does not match archived contents` on reloading existing incident/discovery archives captured with zero bounded funding rows. Preserve original SHA-256 digest and verify its canonical empty-slice input; do not mutate archived JSON or original trading data.
- Add LONG/SHORT JSON round-trip regression and adversarial fee/funding-count/signed-digest mismatch tests.
- Existing order / settlement API remains HTTP 423 LOCKED. Signed REST is not called by offline archive diagnostics.

## v0.4.11-dev.3.88 — 2026-10-23 — pre-PC safety corrections and STOP/HTTP fault injection (LOCKED)

- Strict decimal validation for Binance account unrealized PnL, fail-closed unknown totals and per-position display. Missing position/order arrays no longer imply zero quantity or orders.
- Reject nil model registry explicitly; check canceled context before Demo executor work and POST.
- Local-only fault-injection tests: STOP during final guard, cancellation before POST, simulated POST without ACK (never treated as successful or retryable), controller STOP wait, nil registry, account PnL malformed/missing values.
- Read-only diagnosis only: TP/SL recovery actual mutations, full financial settlement, simultaneous live multi-model execution and Binance Demo E2E **NOT** complete. HTTP 423 remains locked. See `docs/DEV388_PRE_PC_SAFETY_FIXES_KO.md`.

## v0.4.11-dev.3.87 — 2026-10-23 — round 5 Korean read-only model evidence dashboard (LOCKED)

- `GET /api/v1/trading/model-dashboard`: selection, public hypothetical signals, local owner exposure, fail-closed risk/run-control/lifecycle/accounting projection. No credential/private exchange I/O or mutations.
- One-screen Korean model table and unified safety summary; stale generation and unsafe response contracts discarded. No model-level confirmed PnL or inferred zero.
- HTTP 423 remains enforced for auto START/STOP/manual orders; no actual TP/SL/cancel/auto-resume or Demo exchange execution tested.
- New Go read-only and browser DOM contract tests; release scripts/version updated. See `docs/DEV387_READONLY_MODEL_DASHBOARD_KO.md`.

## v0.4.11-dev.3.86 — 2026-10-23 — package C shared multi-model supervised runtime (LOCKED)

- Selected frozen models share ONE coordinator, signed ownership audit, allocated wallet budget, durable lifecycle and auto-run lease; no second exchange execution architecture.
- Serial HOLD -> next model; first order attempt or failure halts the whole run, no subsequent model dispatch, signed review required after adapter ACK. Unexpected outcomes fail closed.
- Multi-model results include per-model audit outcomes. AutoController refuses same-instance restart following supervised dispatch or fail-closed error.
- HTTP manual/auto order routes still 423 LOCKED. No Demo POST, cancel, financial settlement, or actual PC validation in this round. See `docs/DEV386_MULTIMODEL_EXECUTION_KO.md`.

## v0.4.11-dev.3.85 — Package B round 3: bounded TP/SL recovery incident classifier (LOCKED)

- Added pure `ReviewDemoProtectionRecovery` with no order submission, cancellation, source-ledger writing, or authorization: reconciles immutable TP/SL plans, owner quantities, independently signed leg observations, actualOrderId-bound trades and whole-account bounded Demo REST bracket.
- States cover active two-leg observations, triggered sibling needing separate cancellation review, partial exits, double trigger, missing/orphan/foreign Algo orders, and observed flat-side closure (NOT financially posted).
- Rejects same Hedge-side multi-owner legs, duplicate trade IDs, mismatched actual quantities, extra account activity, missing fresh observations, unexpected orders/positions, incomplete signed account arrays or ownership audit.
- Added LONG/SHORT regression and failure-matrix tests; imported local evidence is NOT exchange cryptographic signature nor full historical coverage. Original `MAY_HAVE_SENT`, accounting, HTTP 423 unchanged. Future real Demo signed wiring and operational cancellation/settlement remain unimplemented.

## v0.4.11-dev.3.84 — Package A round 2: immutable cross-ledger settlement barrier (LOCKED)

- Added offline settlement journal `prepare/fence/inspect`: verified dual-ledger preview + independently sealed signed entry archive + current local lifecycle/ownership source snapshot must agree on order, model, side, trade IDs and immutable checksums.
- Durable two-stage chained sidecar, exclusive O_EXCL + fsync, restart/idempotent/concurrent tests; blockers enforce missing fills, unreconciled lifecycle, full history/funding, fresh signed evidence and unavailable real atomic posting.
- Never changes actual lifecycle, ownership, fills, PnL, order routes or Binance positions. Financial settlement still NOT implemented; HTTP 423 unchanged.
- Windows PC actual PowerShell/Demo signed reads pending.

## v0.4.11-dev.3.83 — Package A round 1: offline entry fill recovery manifest (LOCKED)

- Added `PlanPackageAEntryRecovery` / `VerifyPackageAEntryRecoveryPlan`: strictly read-only replay of the existing independently captured entry archive and immutable local journal, one deterministic, SHA-256-sealed candidate per missing signed trade ID.
- Reject duplicate/unrelated/contradictory fills, mutated archives, lifecycle drift, forged approval bits or stale manifest after a signed import; every plan retains historical/funding/atomic-posting and fresh signed recheck blockers.
- Added `plan-entry-recovery` offline signed inspector mode, PowerShell wrapper support and an extra evidence stage in `pc-demo-package-a-collect.ps1` before ambiguous exit lookup. Original evidence never written or deleted.
- **No actual import, lifecycle transition, ownership posting, settlement, signed live PC validation, or HTTP 423 unlock.** Implementation of real atomic reconciliation remains for later rounds/PC verification.

## v0.4.11-dev.3.82 — PC one-command signed evidence collection (READ ONLY; LOCKED)

- `scripts/pc-demo-package-a-collect.ps1`: existing incident SHORT clientOrderID default; tests/compiles signed inspector in isolated temp directory and sequentially captures entry-archive, exit-discovery, and only on unique confirmed exit, discovery-archive plus offline diagnosis. Each stage writes a distinct JSON into pc-results, with a separate collection summary, no keys in logs or outputs.
- Ambiguous, outdated, duplicate, or missing signed trade history returns explicit review-only state; no exit order ID is inferred. Preserve earlier collected signed entry proof even if later steps fail. Server must already be stopped; no original trade file or credential mutation.
- Fix misleading version error message in the existing Package A PowerShell wrapper; retain HTTP 423, lifecycle `MAY_HAVE_SENT`, append-only evidence limits and settlement authorization false.
- Real Windows PowerShell 5.1 and real signed Binance Demo GET require PC verification.

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

## v0.4.11-dev.3.80 (2026-10-22) — Package A discovery-to-incident archive read-only bridge

- Added `discovery-archive`: one read-only command chains unique signed exit candidate search to a second independent signed entry/exit incident audit, storing both under one checksum-sealed JSON (exclusive create, no credentials).
- Added `verify-discovery-archive`: offline validation of bundle linkage, amount/side/order/trade IDs, bounded history, nested archived signed data and immutable original financial journals. All financial mutation/authorization fields are forbidden.
- Added LONG/SHORT linkage tests and ambiguous/tampered/incomplete evidence blockers. Trading remains HTTP 423 LOCKED, old `MAY_HAVE_SENT` unresolved, no financial posting.
- Windows PC/Binance Demo live signed GET remains unverified until operator test.

## v0.4.11-dev.3.79 (2026-10-22) — Package A external exit order ID discovery (READ ONLY)

- Added `-package-a-action exit-discovery`: look up the locally owned signed entry, scan **bounded 14-day** BTCUSDT signed userTrades in disjoint 24h windows, present potential external/manual close exchange `orderId`s, then independently double-check each candidate order with signed GET.
- Only report `UNIQUE_EXIT_ORDER_ID_CANDIDATE_REVIEW_ONLY` when there is exactly one exit with exact matching quantity, no other trades, complete bounded history, a fresh flat Demo snapshot and no open orders. Multiple orders, missing entry fills, truncated/changed history, trade drift, bad financial fields, unfresh/nonflat account, stale local source or lookup errors return a blocked or ambiguous outcome. Candidate IDs **must still be verified by the human operator** before use in `incident-archive`.
- LONG and SHORT use one shared discovery path; no owner/position attribution, financial posting, model-funding attribution, trading authorization, HTTP lock changes or actual orders.
- This tool reduces the user's dependence on manually finding the close order ID. It **does not** resolve the old SHORT `MAY_HAVE_SENT`; full Package A settlement remains blocked pending genuine PC signed proof and crash-safe posting.

## v0.4.11-dev.3.78 (2026-10-22) — Package A signed incident preservation

- Added bounded signed GET incident archive for entry + externally identified exit, preserving original immutable order/fill details, historical userTrades and funding rows.
- Added offline consistency check: order/filled quantities, directions, trade IDs, chronological bounds, full bounded history set, exact fees and funding sum, signed reader provenance digest, local journal drift, checksum and explicit non-authorizing flags.
- Added `incident-archive` / `verify-incident-archive` to the Demo-only signed inspector and PowerShell wrapper; explicit external exit OrderID required. No order or financial posting APIs added.
- Retains historical/lifetime and pooled-funding blockers; requires actual PC signed data for eventual settlement; all HTTP order locks unchanged.

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

## v0.4.11-dev.3.74 — Package A crash-safe NON-POSTING shadow settlement replay (LOCKED)

- Added `ExternalCloseShadowLedger` with strict, offline, append-only prepared → replayed simulated state. Each read checks original proposal, global posting intent, signed two-step replay proof, original trading evidence and the exact entry/exit fill ID and provisional net equation.
- Zero real accounting mutation: flags `applied`, `lifecycle_changed`, `ownership_changed`, `settlement_authorized`, `trading_authorized`, `funding_verified`, `history_complete` are always false; original SHORT MAY_HAVE_SENT and HTTP423 are preserved.
- Added `cmd/demo-close-shadow-replay`, PowerShell 5.1 offline wrapper, LONG/SHORT concurrent idempotency (30), crash restart, replay, poisoned/missing/symlink records and active runtime lock tests.
- Full funding allocation, historical coverage, actual signed PC evidence and production atomic multi-journal posting remain unimplemented. No trading authorization.

## v0.4.11-dev.3.73 — Package A second integration: signed bounded coverage and NON-POSTED global posting intent (LOCKED)

- Signed Binance Demo read-only entry/exit audit now requires the **last entry fill** to precede the **first exit fill**, validates each fill's positive price, and reports exact entry→exit time boundaries and trade/funding request counts. This remains bounded, NOT full account funding or model allocation proof.
- `external_close_posting_intent.go`: strict single global immutable append-only `RESERVED_NOT_POSTED` transaction-identity fence binds proposal, two independent signed replay checkpoint hashes and all exchange fill IDs. Crash/restart, 30 concurrent calls, tamper, extra files, active lock, original data drift and forged authorization tests fail closed; no financial posting is implemented.
- `cmd/demo-close-posting-intent` and `scripts/pc-demo-close-posting-intent.ps1` provide offline inspection / explicit non-posting reservation after both signed rechecks. They do not read API keys or place orders.
- Critical limitations: full historical trade/funding/model attribution proof and atomic cross-ledger financial posting **remain unavailable**. Existing SHORT MAY_HAVE_SENT, trading lock HTTP423 and all original trading records unchanged. Real Binance Demo PC test pending.

## v0.4.11-dev.3.72 — Package A signed GET revalidation + non-posted replay checkpoint (LOCKED)

- Demo signed GET entry/exit order/fills double read, bounded whole BTCUSDT trades and funding-incomes, exact identity/financial digest; no unproven full-history/pooled-Hedge attribution claims.
- Durable, append-only, fail-closed stage/recheck checkpoint stored separately from original `data/trading` execution/ownership. Restart from STAGED with another signed GET; no financial posting.
- `demo-signed-inspect -replay-action probe|stage|recheck`, PowerShell wrapper, mock REST and crash/race/tamper tests. Real Demo and PC execution **not yet verified**; HTTP423 trading locked.
- Package A **partial**: complete funding/account-history proof and transactional settlement journal are still outstanding; historic SHORT MAY_HAVE_SENT left intact.

## v0.4.11-dev.3.71 — signed 재검증 계약 및 재실행 식별 (LOCKED)

- `internal/trading/external_close_revalidation.go`: 독립 signed 증거 조회를 위한 `SignedCloseEvidenceReader` 인터페이스, 진입/청산 주문·체결 ID 및 최신 flat 계정 검증, deterministic replay key 및 checksum-sealed 비승인 결과를 추가.
- LONG/SHORT 양쪽 재시도 동일성, 응답 오류, 변경된 체결 ID, 오래된 증거, 허가 위조를 테스트한다.
- **주의:** 실제 Binance signed REST 어댑터, 적용 journal/writer, crash-safe 중단 재개, 펀딩/전체 이력 독립 증명은 아직 미구현. 이 계약은 실제 정산 상태 전이가 아니다. Lifecycle/Ownership 데이터 수정, 거래 잠금 해제는 금지.

## v0.4.11-dev.3.70 — 외부 청산 정산 적용 사전 게이트 (LOCKED)

- `internal/trading/external_close_posting_gate.go`: 봉인된 기존 미적용 제안/운영자 검토를 바탕으로 7가지 독립 증거 및 crash-safe 적용 조건을 읽기 전용으로 점검하고 항목별 차단 이유와 지문을 제공한다.
- LONG/SHORT 공통 조건을 테스트하고 거래 허가, Lifecycle/Ownership 기록 수정, 실제 POST 적용은 전부 **금지**. 운영자 검토 확인은 정산 승인이 아니며, 실제 거래소 signed 검증/펀딩 배분/중단 복구 정산 엔진은 후속 작업이다.
- 이 버전은 정산 적용 프로토콜의 **preflight 계약**만 추가한다. `MAY_HAVE_SENT`, 거래 데이터 및 주문 423 잠금 유지.

## v0.4.11-dev.3.69 — 외부 청산 미적용 정산 조정안 (LOCKED)

- `external_close_settlement_proposal.go`: dev.3.68의 운영자 검토 ACK와 원본 증거를 재검증하여 거래 주문·체결 ID, 모델별 LONG/SHORT 수량 조정 예정값, 진입/청산 수수료, 펀딩 제외 손익을 append-only **미적용 제안**으로 봉인. 검토 지문·signed 조회 로컬 지문·원본 지문 연결.
- 같은 사건 재시도·재시작 멱등, 충돌 사건·양방향 계정 공유 청산 이중 귀속 차단, 원본/검토 변조·미종결 주문 잠금 중 생성 차단. 별도 도구 `cmd/demo-close-settlement-proposal`와 `scripts/pc-external-close-proposal.ps1` 제공.
- **펀딩과 역사 전체 검증 미완료, 실제 정산 적용·Lifecycle/Ownership/PnL 원본 변경 전부 금지, 거래 잠금 유지**. 운영자 ACK와 SHA-256은 Binance 서명 인증 아님. 상세 `docs/DEV369_EXTERNAL_CLOSE_SETTLEMENT_PROPOSAL_KO.md`.

## v0.4.11-dev.3.68 — append-only 운영자 복구 검토 이력 (LOCKED)

- `internal/trading/external_close_review_ledger.go`: 오프라인 사건별 `REVIEW_STAGED` 및 단일 `OPERATOR_ACKNOWLEDGED_REVIEW_ONLY` 또는 `OPERATOR_REJECTED_REVIEW_ONLY` 이벤트; 원본 evidence digest, 리허설 checksum, 이전 이벤트 checksum으로 봉인, O_EXCL 파일 + fsync. 동시 호출·중단 후 재시도·증거 변경·변조 차단.
- `cmd/demo-close-review-ledger`, `scripts/pc-external-close-review.ps1`: PC 서버 종료 시에만 보조 기록을 선택적으로 생성/조회/확인. 기본은 조회이며 쓰기는 명시적인 `REVIEW_ONLY_NOT_SETTLEMENT` 확인 필요. 별도의 signed GET이나 거래소 주문은 호출하지 않음.
- 검토 장부의 읽기 전용 조회(`Load`)도 원본 실행 증거 지문을 다시 확인해, 검토 이후 체결 파일이 누락·변경된 경우 과거 검토 상태를 현재 유효한 결과로 표시하지 않음.
- 실제 복구(`MAY_HAVE_SENT`, OwnershipStore, fills, reservation), 모델별 펀딩/손익 확정, 주문·자동매매 잠금 해제 **전부 미구현/금지**. 실제 과거 SHORT 사고 정산되지 않음.

## v0.4.11-dev.3.67 — signed 외부 청산 비파괴 복구 리허설 (LOCKED)

- `internal/trading/external_close_rehearsal.go`: LONG/SHORT 외부 청산 후보가 독립 signed 진입/청산 기록, 로컬 원본 체결과 일치할 때만 메모리에서 수량·수수료·실현손익을 재계산하며 잔여 수량 0을 확인하는 READ-ONLY 리허설을 추가했다.
- 기존 signed 감사 JSON에 `external_close_rehearsals`를 추가했다. `local_evidence_sha256`은 감사 시작 시 로컬 파일 스냅샷 해시이고, `signed_read_digest_sha256`은 감사 중 확인한 기록들의 로컬 지문이다. Binance 발행 전자서명·거래 허가 증명이 아니다.
- `entry` 로컬 체결 누락, Binance signed 정보 불일치, 역사 조회 누락, 변조·동시 변경, USDT 외 수수료는 리허설을 차단한다. 후속 작업에는 운영자 확인, append-only 외부 청산 장부 및 재시작 시 중복 반영 방지, 펀딩·전체 이력 재검증이 필요하다.
- `MAY_HAVE_SENT`, 기존 소유권/예약 및 모든 거래 잠금은 그대로 유지한다. 복구 리허설은 원본 `data/`를 쓰거나 Binance 주문을 전송하지 않는다. 기존 `dev.3.66`의 감사 결과/구조와 호환한다.

## v0.4.11-dev.3.66 — LONG/SHORT signed 외부 청산 후보 대조 (LOCKED)

- `SignedLedgerHistoryReport.external_close_recovery_cases`: 기존 checksum-sealed entry와 외부 signed orderId로 재조회한 청산 주문을 Hedge 방향별(`SELL/LONG`, `BUY/SHORT`)로 대조하는 **읽기 전용 후보 판정기** 추가. 정확한 체결 합계, 체결 시각 순서, signed 순손익 원천(realizedPnL)과 USDT 청산 수수료, 14일 bounded history, fresh Demo flat account, open standard/algo orders 부재를 요구.
- 소유 주문이 다수이거나 외부 주문이 2개 이상, 부분/초과 청산, 다른 방향, 미완료 이력, 상태 변동, 오래된 snapshot, 계정 포지션 미정리, 손상 데이터가 있으면 `INSUFFICIENT_OR_AMBIGUOUS_EVIDENCE`. signed 진입은 확인되었으나 로컬 체결이 누락된 경우에도 분리된 blocker를 남김.
- double signed external orderId 조회 시 두 번째 심볼/positionSide/type 변경 감지를 강화. 감사 도중 로컬 증거가 달라지면 후보도 무효화. 단일 계정 통합 모델 소유권, 펀딩 및 이력 이전의 전체 정산은 미검증.
- **회계·Lifecycle·예약·보호 주문 원본 변경 없음, 자동 복구 없음, trading_authorized=false, HTTP 423 잠금 유지.** 과거 SHORT 사고를 ‘이미 정산됨’으로 해석하지 않는다. 자세한 제약은 `docs/DEV366_EXTERNAL_CLOSE_RECOVERY_CANDIDATES_KO.md`.

## v0.4.11-dev.3.65 — 독립 signed 개별 주문/체결/외부 거래 감사 (LOCKED)

- 기존 dev.3.64 읽기 전용 증거 대조를 보완한 `trading.ReviewSignedLedgerHistory`: 로컬 checksum-sealed 주문 전체에 대해 Binance Demo signed 주문 GET → 해당 주문 userTrades GET → signed 주문 GET을 수행하고 주문 신원/상태, 체결 합계, 로컬 ID·수량·가격·수수료·실현손익/시각을 정확한 십진수로 비교.
- 가장 오래된 signed owned 체결을 기준으로 최대 14일, 24시간 분할 signed account trade history를 조회하여 거래소 BTCUSDT 체결이 실제 로컬 주문에 귀속되는지 대조. 외부/수동 주문은 `unattributed_trades` 및 별도의 두 번 signed orderId 검증 결과(`external_orders`)로 **검토 후보만** 보고하며 절대 모델 소유권을 자동 배정하지 않음.
- 기간 미포함, 1000건 상한, 주문 drift, 로컬 fill 누락/변조, 미정산 lifecycle·예약, 장부/계정 불일치, symlink, 동시 데이터 수정 등을 차단. 사건 전 과거 내역, 펀딩, 보호주문 체결의 완전한 재구성/정산은 미보증.
- `cmd/demo-signed-inspect -ledger` 및 Windows `scripts/pc-demo-ledger-audit.ps1` 추가. **서버 종료 상태에서만 `.enc`를 로컬로 읽어 Demo signed GET**을 실행하고 `pc-results`에 새 JSON을 O_EXCL로 생성. 원본 `data/`와 주문/lifecycle 기록은 변경하지 않음.
- `trading_authorized=false`, `local_lifecycle_changed=false` 상시 고정, 웹 주문 API HTTP 423 유지. 실제 Binance Demo 서명 조회는 PC에서만 테스트 가능. 상세 `docs/DEV365_SIGNED_LEDGER_FORENSICS_KO.md`.

