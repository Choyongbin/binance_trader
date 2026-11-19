// Command model-registry-audit inspects compiled model identities without
// connecting to Binance, opening trading stores or modifying settings.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"binance_trader/internal/app"
	"binance_trader/internal/model"
)

func main() {
	selected := flag.String("selected", "", "comma-separated model IDs to audit (optional)")
	settings := flag.String("settings", "", "explicit existing settings.json path (read only; optional)")
	flag.Parse()
	if flag.NArg() != 0 || (*selected != "" && *settings != "") {
		fmt.Fprintln(os.Stderr, "-settings와 -selected는 동시에 사용할 수 없습니다")
		os.Exit(2)
	}
	var ids []string
	if *settings != "" {
		loaded, err := app.ReadModelSettingsOnly(*settings)
		if err != nil {
			fmt.Fprintln(os.Stderr, "settings 읽기 차단:", err)
			os.Exit(2)
		}
		ids = loaded.SelectedModelIDs
	} else if *selected != "" {
		ids = strings.Split(*selected, ",")
		for i := range ids {
			ids[i] = strings.TrimSpace(ids[i])
		}
	}
	audit := app.AuditModelConfiguration(model.DefaultRegistry(), ids)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(audit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !audit.CatalogRegistryConsistent || len(audit.Blockers) > 0 {
		os.Exit(2)
	}
}
