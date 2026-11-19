package main

import (
	"binance_trader/internal/releaseupdate"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	src := flag.String("source", "", "압축 해제된 새 누적 소스 폴더")
	dst := flag.String("target", "", "기존 설치 폴더 또는 새 폴더 경로")
	apply := flag.Bool("apply", false, "백업을 생성하고 설치를 실제 적용")
	fresh := flag.Bool("new", false, "기존 설치를 건드리지 않고 새 폴더에 설치")
	jsonOut := flag.Bool("json", false, "machine-readable JSON plan")
	restore := flag.String("restore-backup", "", "검증된 complete backup을 target으로 복구")
	flag.Parse()
	if *restore != "" {
		quarantine, err := releaseupdate.RestoreCompleteBackup(*restore, *dst)
		if err != nil {
			fmt.Fprintln(os.Stderr, "STOP / FAIL:", err)
			os.Exit(1)
		}
		if *jsonOut {
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"restored": true, "target": *dst, "quarantine": quarantine})
		} else {
			fmt.Println("RESTORE PASS. 실패 설치 격리:", quarantine)
		}
		return
	}
	plan, err := releaseupdate.Run(releaseupdate.Options{Source: *src, Target: *dst, Apply: *apply, NewInstall: *fresh})
	if err != nil {
		if *jsonOut {
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"ok": false, "plan": plan, "error": err.Error()})
		} else {
			if plan.Backup != "" {
				fmt.Fprintln(os.Stderr, "원본 전체 백업:", plan.Backup)
			}
			fmt.Fprintln(os.Stderr, "STOP / FAIL:", err)
		}
		os.Exit(1)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"ok": true, "plan": plan})
		return
	}
	fmt.Println("검증된 누적 릴리스 파일 수:", plan.Files)
	fmt.Println("대상:", plan.Target)
	if !*apply {
		fmt.Println("DRY RUN PASS: 파일은 변경하지 않았습니다. 실제 적용은 -apply 필요.")
		return
	}
	if plan.Backup != "" {
		fmt.Println("이전 설치 전체 백업 (data/.git 포함):", plan.Backup)
	}
	fmt.Println("APPLY PASS: 소스 파일 해시 재검증 완료. 기존 data/.git은 변경하지 않았습니다.")
}
