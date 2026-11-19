package trading

// Read-only inventory of the durable reservation ledger used as the pooled
// Hedge-side serialization fence. This is diagnostic only: it never creates,
// settles, removes or authorizes a reservation/order.
import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	PooledExitReviewEmpty          = "EMPTY"
	PooledExitReviewClear          = "CLEAR_NO_EXIT_FENCE"
	PooledExitReviewEntriesPending = "ENTRY_MUTATIONS_PENDING"
	PooledExitReviewFenced         = "SERIALIZED_EXIT_IN_FLIGHT"
	PooledExitReviewConflict       = "LEGACY_CONCURRENT_MUTATION_BLOCKED"
)

type PooledSideSerialization struct {
	Side               PositionSide `json:"side"`
	ActiveEntries      int          `json:"active_entries"`
	ActiveExits        int          `json:"active_exits"`
	Completed          int          `json:"completed"`
	ActiveClientOrders []string     `json:"active_client_orders"`
	Blockers           []string     `json:"blockers"`
}

type PooledExitSerializationReview struct {
	State                 string                    `json:"state"`
	Sides                 []PooledSideSerialization `json:"sides"`
	ActiveReservations    int                       `json:"active_reservations"`
	CompletedReservations int                       `json:"completed_reservations"`
	TradingAuthorized     bool                      `json:"trading_authorized"`     // ALWAYS false
	ManualReviewRequired  bool                      `json:"manual_review_required"` // ALWAYS true
}

func reviewPooledExitReservations(c *Coordinator, all []Reservation) PooledExitSerializationReview {
	r := PooledExitSerializationReview{State: PooledExitReviewEmpty, ManualReviewRequired: true, Sides: []PooledSideSerialization{}}
	bySide := map[PositionSide]*PooledSideSerialization{
		Long:  {Side: Long, Blockers: []string{}, ActiveClientOrders: []string{}},
		Short: {Side: Short, Blockers: []string{}, ActiveClientOrders: []string{}},
	}
	for _, reservation := range all {
		s := bySide[reservation.Intent.PositionSide]
		if s == nil {
			continue // decodeReservation already validates intent sides
		}
		if c.isDone(reservation) {
			s.Completed++
			r.CompletedReservations++
			continue
		}
		r.ActiveReservations++
		s.ActiveClientOrders = append(s.ActiveClientOrders, reservation.ClientOrderID)
		if reservation.Intent.Action == Exit {
			s.ActiveExits++
		} else {
			s.ActiveEntries++
		}
	}
	severity := 0 // EMPTY 0, CLEAR 1, ENTRY 2, FENCE 3, CONFLICT 4
	if len(all) > 0 {
		severity = 1
		r.State = PooledExitReviewClear
	}
	for _, side := range []PositionSide{Long, Short} {
		s := bySide[side]
		sort.Strings(s.ActiveClientOrders)
		if s.ActiveExits > 1 || s.ActiveExits > 0 && s.ActiveEntries > 0 {
			s.Blockers = append(s.Blockers, "구버전 또는 비정상 상태에서 동일 방향 청산과 다른 mutation이 동시에 미종료 상태입니다")
			if severity < 4 {
				severity = 4
				r.State = PooledExitReviewConflict
			}
		} else if s.ActiveExits == 1 {
			s.Blockers = append(s.Blockers, "직렬 청산 fence 활성: 서명 주문·체결·전체 포지션 대조 후 Settle 전까지 같은 방향 청산/재진입 금지")
			if severity < 3 {
				severity = 3
				r.State = PooledExitReviewFenced
			}
		} else if s.ActiveEntries > 0 {
			s.Blockers = append(s.Blockers, "미종료 진입 예약 존재: 해당 방향의 새 청산은 모든 진입 Settle 전까지 금지")
			if severity < 2 {
				severity = 2
				r.State = PooledExitReviewEntriesPending
			}
		}
		if s.ActiveEntries+s.ActiveExits+s.Completed > 0 {
			r.Sides = append(r.Sides, *s)
		}
	}
	return r
}

func validPooledReservationArchiveEntry(parent, name string, isDir bool) bool {
	if parent != "" || isDir {
		return false
	}
	if strings.HasSuffix(name, ".done.json") {
		base := strings.TrimSuffix(name, ".done.json") + ".json"
		return reservationName.MatchString(base)
	}
	return reservationName.MatchString(name)
}

// ReviewPooledExitSerialization opens no coordinator lock and writes nothing.
// A missing reservation directory is a clean EMPTY diagnostic. Any unknown,
// corrupt, orphan, symlink or concurrently changing file fails closed instead
// of being skipped or repaired.
func ReviewPooledExitSerialization(dataDir string) (PooledExitSerializationReview, error) {
	if dataDir == "" {
		return PooledExitSerializationReview{}, errors.New("데이터 디렉터리가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "reservations")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return PooledExitSerializationReview{State: PooledExitReviewEmpty, ManualReviewRequired: true, Sides: []PooledSideSerialization{}}, nil
	}
	if err != nil {
		return PooledExitSerializationReview{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return PooledExitSerializationReview{}, errors.New("예약 감사 경로가 안전한 디렉터리가 아닙니다")
	}
	before, _, _, err := snapshotDir(dir, validPooledReservationArchiveEntry)
	if err != nil {
		return PooledExitSerializationReview{}, err
	}
	c := &Coordinator{reserveDir: dir}
	all, err := c.scan()
	if err != nil {
		return PooledExitSerializationReview{}, err
	}
	review := reviewPooledExitReservations(c, all)
	after, _, _, err := snapshotDir(dir, validPooledReservationArchiveEntry)
	if err != nil {
		return PooledExitSerializationReview{}, err
	}
	if before != after {
		return PooledExitSerializationReview{}, errors.New("pooled exit 감사 중 예약 파일이 변경되었습니다: 중지된 오프라인 사본에서 다시 검토하십시오")
	}
	return review, nil
}
