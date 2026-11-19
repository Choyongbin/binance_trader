package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"binance_trader/internal/server"
)

func main() {
	listen := flag.String("addr", "127.0.0.1:8080", "로컬 서버 주소 (루프백만 허용)")
	data := flag.String("data", "./data", "로컬 설정 및 인증 파일 디렉터리")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatal("주소 형식 오류: ", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		log.Fatal("보안상 루프백 주소(127.0.0.1 / localhost / ::1)만 허용합니다")
	}
	app, err := server.New(*data)
	if err != nil {
		log.Fatal("시작 실패: ", err)
	}
	defer app.Close()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal("포트 열기 실패: ", err)
	}
	app.StartMarket() // market stream never needs private Demo credentials
	httpServer := &http.Server{
		Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 45 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 16,
	}
	go func() {
		log.Printf("바이낸스 트레이더 통합 UI 실행: http://%s (거래 기능 비활성화)", listener.Addr())
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("서버 오류: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil {
		log.Printf("종료 오류: %v", err)
	}
}
