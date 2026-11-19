// report-index is an explicit operator-only CLI. It reads existing checksum-
// verified immutable accounting reports and builds a disposable SQLite query
// projection; it never reads API keys, contacts Binance or submits orders.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"binance_trader/internal/reportindex"
)

func main() {
	data := flag.String("data", "./data", "로컬 데이터 폴더 (거래소 인증 정보는 읽지 않음)")
	verify := flag.Bool("verify-only", false, "SQLite 원본 지문 및 무결성만 확인")
	flag.Parse()
	var status reportindex.Status
	var err error
	if *verify {
		status, err = reportindex.StatusOnly(*data)
	} else {
		status, err = reportindex.Build(*data)
	}
	if err != nil {
		log.Printf("검사 실패: %v; 원본 기록은 변경하지 않았습니다", err)
		os.Exit(1)
	}
	fmt.Printf("회계 SQLite 읽기 색인: %s / 보고서 %d개 / 거래 권한=false / 순손익 확정=false\n", status.State, status.ReportCount)
	fmt.Println("주의: 과거에 별도 생성된 보고서만 조회합니다. 실제 체결 원본, 수수료 환산, 펀딩 증거가 자동 수집되지는 않습니다.")
}
