Binance Futures Demo 전용 읽기 전용 서명 주문 진단 패치 (dev.3.55)

반드시 트레이더 서버를 먼저 .\stop.ps1로 종료하세요.
이 ZIP은 기존 파일을 교체하지 않고 두 개의 신규 파일만 추가합니다.
  cmd/demo-signed-inspect/main.go
  scripts/pc-demo-signed-inspect.ps1

안전 절차:
1) F:\binance_trader 원본 폴더의 data/를 삭제/편집하지 마세요.
2) ZIP을 F:\binance_trader 에 압축 해제 (기존 파일 덮어쓰기 불필요).
3) cd F:\binance_trader
4) .\stop.ps1
5) .\scripts\pc-demo-signed-inspect.ps1
6) pc-results\signed-order-review-*.json 최신 파일 첨부

특성: Demo only, signed GET order + signed account snapshot + signed userTrades,
최소한의 결과만 로컬 JSON으로 기록. 주문 POST, 취소 DELETE, 데이터 수정,
라이프사이클 정리, 자동 주문 재시도, 자동 거래 허가는 절대 수행하지 않음.
특정 pending ID bt_06893a0c50b6928dede7365cd2f0 한 건만 조회.
주문이 조회되지 않아도 발주 실패의 확정 증거가 아님. 반복 테스트 금지.
