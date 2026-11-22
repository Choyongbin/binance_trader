# Binance Trader — v11 독립 후속 패치: 제품 보호주문 Write-Ahead Mutation Fence

기준 GitHub `main`: `738d628543a587255e5d11219bf4adb1bd018553` (`VERSION`: `v0.4.11-dev.3.138`).

> **중요:** 이 패치는 v1~v10 누적 패치를 포함하지 **않는 별도 추가분**입니다.
> 이전 v10 누적 패치의 실제 파일이 이번 개발 환경에 없어, v10 `ReconcileEntryOnce()`와의 최종 통합 검증은 수행하지 못했습니다.
> 파일을 임의로 재구성해 '누적 v11'이라고 표시하지 않았습니다.

## 추가 모듈

- `internal/protectionfence/fence.go`: 보호주문 제출(TP, SL), 발동 형제주문 취소(DELETE)를 위한 단일 계정 수준 비가역 write-ahead 기록.
- `internal/trading/product_mutation_fence_bridge.go`: 기존 `DemoProtectionRuntimeCoordinator`의 독점 lease 및 영속 `ProtectionDispatchPlan` 일치 확인 후 오프라인 펜스 기록.
- `internal/protectionfence/fence_test.go`: 재시작, 중복, ACK 불명, 동시 프로세스/다른 레그, 서로 다른 그룹, 체크섬 변조, orphan·symlink·partial write, 잘못된 숫자/ID/계정 및 secret 미저장 테스트.

## 동작 제한 및 안전 보증 범위

1. 첫 `SealBeforePossibleSend()`는 `data/trading/product_mutation_fences/unreconciled-mutation.json`을 `O_EXCL`, `0600`, `Sync()`로 먼저 기록합니다. 결과 `MAY_HAVE_SENT_RECOVERY_ONLY`는 *사고 가능성의 증거*일 뿐 거래소 전송 허가가 아닙니다.
2. 한 건이라도 펜스가 존재하면 **다른 그룹·다른 레그 포함 모든 신규 mutation은 차단**합니다. 독립 signed GET·영속 연속성 증명으로 상태를 복구하고 다음 실행으로 넘어가는 로직은 **아직 없습니다**. 기존 펜스 파일을 임의로 삭제하거나 이동해서 진행하지 마세요.
3. 기존 `Arm()` / `CancelTriggeredSibling()` 및 제품 HTTP `423 LOCKED`는 수정하지 않았습니다. Binance API POST/DELETE, 금융 원장 COMMIT, 실제 계정 보호주문은 한 번도 수행하지 않습니다.
4. SHA-256 증거 식별자는 서명/소유권/과거 이력의 진실성을 검증하지 않습니다. 프로세스 외부에서 독립 검증해야 합니다. 검증 없이 이 API의 성공을 주문 승인으로 해석하지 마세요.
5. 파일의 Checksum은 우발적 손상 탐지일 뿐 악의적인 로컬 관리자/스토리지 공격을 막는 인증 도구가 아닙니다. 파일시스템·디렉터리 메타데이터 및 각 OS의 Sync 보장을 별도로 검증해야 합니다. NAS, 네트워크 파일시스템에서는 원자적 O_EXCL을 운영 보증하지 않습니다.
6. 펜스는 미래 실행기와 연결할 *별도 구성요소*이며 v10 원본에 실제 호출부는 없습니다. 사용자가 실행 중인 서버/자동매매 동작은 변하지 않습니다.

## 검증 범위

- 단독 신규 모듈: `GO111MODULE=off go test -race ./internal/protectionfence -count=1` PASS.
- 어댑터: **테스트용 정의 스텁에서만** 컴파일 및 단위 smoke PASS. GitHub 실제 `738d628` 전체 소스 빌드 PASS를 뜻하지 않습니다.
- 신규 모듈·어댑터 혼합 테스트 디렉터리: `go vet ./internal/...` PASS.
- Windows/amd64 신규 패키지 및 어댑터 테스트 바이너리 크로스 컴파일 PASS.
- 전체 `go test ./...`, 실제 GitHub 전체 HEAD 빌드/PC 적용, 바이낸스 Demo signed REST/Private WS·주문 검증: **미수행**.

## PC 복귀 후 순서 (지금 PC에서 할 일 없음)

1. `git rev-parse HEAD`로 기준 커밋 확인.
2. 원본 v10 누적 패치를 먼저 `git apply --check` 후 적용하고 테스트. v1~v9를 다시 적용하면 중복됩니다.
3. 이 v11 **추가분**을 `git apply --check binance-trader-auto-core-738d628-v11-additive.patch`로 확인 후 적용.
4. `go test ./...`, `go vet ./...`, Windows 빌드, v10 `ReconcileEntryOnce()`와 중복 기록 및 전송 경계의 정확한 통합 리뷰 실시.
5. signed reconciliation, 계정 전체 소유권/과청산 방지 및 실제 보호주문 감독 E2E가 입증되기 전까지 HTTP 423을 풀지 마세요.
