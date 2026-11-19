package main

import (
	"binance_trader/internal/pccheck"
	"binance_trader/internal/version"
	"fmt"
	"os"
	"runtime"
)

func main() {
	fmt.Println("Release:", version.Release, "Trading:", version.Trading, "Platform:", runtime.GOOS)
	if version.Trading != "LOCKED" {
		fmt.Fprintln(os.Stderr, "FAIL: 거래 잠금 해제 버전입니다")
		os.Exit(1)
	}
	if err := pccheck.SQLiteTempRoundTrip(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL SQLite:", err)
		os.Exit(1)
	}
	fmt.Println("PASS: 실제 플랫폼 SQLite 임시 DB 쓰기/한글/파라미터/읽기전용 조회 정상 (사용자 data/ 미접근)")
}
