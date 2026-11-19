//go:build !windows && (!linux || !cgo)

package sqlitebridge

// Unsupported platforms/CGO-disabled Linux can still build and serve the
// rest of the existing read-only application. Projection creation/verification
// fails closed rather than silently introducing a mock database.
import "errors"

var errUnsupported = errors.New("이 빌드에서는 SQLite 네이티브 드라이버가 비활성화되었습니다 (Linux에서는 CGO+libsqlite3가 필요합니다)")

type DB struct{}

func Available() bool                                       { return false }
func Open(_ string, _ bool) (*DB, error)                    { return nil, errUnsupported }
func (*DB) Close() error                                    { return nil }
func (*DB) Exec(_ string, _ ...string) error                { return errUnsupported }
func (*DB) Query(_ string, _ ...string) ([][]string, error) { return nil, errUnsupported }
