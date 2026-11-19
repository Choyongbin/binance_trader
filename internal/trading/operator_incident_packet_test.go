package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorIncidentPacketsEmptyAreReadOnlyAndSealed(t *testing.T) {
	root := t.TempDir()
	got, err := BuildOperatorIncidentPackets(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.CriticalPacketCount != 0 || len(got.Packets) != 0 || got.TradingAuthorized || got.AutomaticRepair || got.MutationPerformed || !got.ManualReviewRequired {
		t.Fatalf("unsafe empty packet set: %+v", got)
	}
	if len(got.EvidenceSHA256) != 64 || len(got.SHA256) != 64 || got.SHA256 != got.digest() {
		t.Fatalf("unsealed packet set: %+v", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("packet generation mutated data tree: %+v", entries)
	}
}

func TestOperatorIncidentPacketsInterruptedRecoveryIncludesOnlyDigestMetadata(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "recovery_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := RecoveryProbe{Schema: 1, ID: "0123456789abcdef0123456789abcdef", Stage: "STARTED", StartedAt: time.Now().Add(-time.Minute).UTC(), Blockers: []string{}, ManualReviewRequired: true}
	p.seal()
	raw, _ := json.MarshalIndent(p, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, p.ID+".start.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}

	got, err := BuildOperatorIncidentPackets(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got.CriticalPacketCount < 1 {
		t.Fatalf("packets=%+v", got.Packets)
	}
	var packet OperatorIncidentPacket
	found := false
	for _, candidate := range got.Packets {
		if candidate.Code == "RECOVERY_PROBE_INTERRUPTED" {
			packet = candidate
			found = true
			break
		}
	}
	if !found || packet.Priority != RemediationCritical || packet.PacketID == "" || packet.SHA256 != packet.digest() {
		t.Fatalf("bad recovery packet: %+v", got.Packets)
	}
	if packet.TradingAuthorized || packet.AutomaticRepair || packet.MutationPerformed || !packet.ManualReviewRequired || packet.Scope != IncidentPacketScope {
		t.Fatalf("unsafe packet: %+v", packet)
	}
	if len(packet.EvidenceRefs) != 1 || packet.EvidenceRefs[0].Kind != "recovery_probe" || packet.EvidenceRefs[0].ID != p.ID || len(packet.EvidenceRefs[0].SHA256) != 64 {
		t.Fatalf("bad refs: %+v", packet.EvidenceRefs)
	}
	body, _ := json.Marshal(packet)
	for _, forbidden := range []string{"api_key", "secret", "signature", "passphrase"} {
		if strings.Contains(strings.ToLower(string(body)), forbidden) {
			t.Fatalf("sensitive material leaked: %s", forbidden)
		}
	}
}

func TestOperatorIncidentPacketIDStableAcrossGenerationTime(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "recovery_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := RecoveryProbe{Schema: 1, ID: "fedcba9876543210fedcba9876543210", Stage: "STARTED", StartedAt: time.Unix(1700000000, 0).UTC(), Blockers: []string{}, ManualReviewRequired: true}
	p.seal()
	raw, _ := json.MarshalIndent(p, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, p.ID+".start.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	a, err := BuildOperatorIncidentPackets(root, time.Unix(1800000000, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildOperatorIncidentPackets(root, time.Unix(1800000600, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	ids := func(set OperatorIncidentPacketSet) map[string]string {
		m := map[string]string{}
		for _, packet := range set.Packets {
			m[packet.Code] = packet.PacketID
		}
		return m
	}
	ia, ib := ids(a), ids(b)
	if len(ia) != len(ib) {
		t.Fatalf("packet count changed: %v %v", ia, ib)
	}
	for code, id := range ia {
		if ib[code] != id {
			t.Fatalf("packet id changed for %s: %s != %s", code, id, ib[code])
		}
	}
	if a.SHA256 == b.SHA256 {
		t.Fatal("packet-set SHA should include generation time")
	}
}

func TestIncidentPacketCoversPreStartPrivateDiagnosticLock(t *testing.T) {
	root := t.TempDir()
	writeDiagnosticOwnerForTest(t, root, "recovery_sessions", "0123456789abcdef0123456789abcdef")
	set, err := BuildOperatorIncidentPackets(root, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, packet := range set.Packets {
		if packet.Code != "PRIVATE_DIAGNOSTIC_LOCK" {
			continue
		}
		if !packet.Snapshot.RecoveryProbeLockPresent || packet.TradingAuthorized || packet.AutomaticRepair || len(packet.EvidenceRefs) != 1 || packet.EvidenceRefs[0].Kind != "recovery_probe_lock" {
			t.Fatalf("unsafe diagnostic lock packet: %+v", packet)
		}
		return
	}
	t.Fatalf("missing diagnostic lock incident packet: %+v", set)
}
