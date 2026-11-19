package wsclient

// SafeDialDiagnostic is a strict, allowlisted summary of the outbound WebSocket
// connection attempt. Raw URLs (which may contain a listenKey), TLS errors,
// response headers/bodies and platform network errors are NEVER retained.
import "errors"

type SafeDialDiagnostic struct {
	Phase      string `json:"dial_phase,omitempty"`
	HTTPStatus int    `json:"dial_http_status,omitempty"`
}

type dialError struct{ diagnostic SafeDialDiagnostic }

func (e *dialError) Error() string { return "WebSocket connection failed at " + e.diagnostic.Phase }

func failedDial(phase string, code int) error {
	return &dialError{diagnostic: SafeDialDiagnostic{Phase: phase, HTTPStatus: code}}
}

// DiagnoseDial never formats an upstream error, including arbitrary errors
// returned by injected dialers. Only errors manufactured by this package can
// carry a phase/status. Even an errors.Join wrapping a raw secret is safe here.
func DiagnoseDial(err error) SafeDialDiagnostic {
	var e *dialError
	if errors.As(err, &e) && e != nil {
		return e.diagnostic
	}
	return SafeDialDiagnostic{Phase: "UNCLASSIFIED"}
}

// RedactDial discards ALL original causes before a failure crosses private
// account observer boundaries or is stored on disk.
func RedactDial(err error) error {
	d := DiagnoseDial(err)
	return failedDial(d.Phase, d.HTTPStatus)
}

func ValidDialDiagnostic(d SafeDialDiagnostic) bool {
	switch d.Phase {
	case "":
		return d.HTTPStatus == 0 // historical records have no metadata
	case "HTTP_STATUS":
		return d.HTTPStatus >= 100 && d.HTTPStatus <= 599 && d.HTTPStatus != 101
	case "CONNECTED", "URL_VALIDATION", "TCP_DIAL", "TLS_HANDSHAKE", "UPGRADE_WRITE", "UPGRADE_RESPONSE", "UPGRADE_HEADERS", "UPGRADE_ACCEPT", "UNCLASSIFIED", "TIMEOUT":
		return d.HTTPStatus == 0
	default:
		return false
	}
}
