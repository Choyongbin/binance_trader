package server

import (
	"net/http"
)

func (s *Server) autoControlStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "쿼리 파라미터를 지원하지 않습니다")
		return
	}
	if s.autoControl == nil {
		respondError(w, http.StatusServiceUnavailable, "자동매매 run control이 초기화되지 않았습니다")
		return
	}
	status, err := s.autoControl.Status()
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"control":            status,
		"trading_enabled":    false,
		"automatic_resume":   false,
		"start_route_locked": true,
		"stop_route_locked":  true,
		"mutation_performed": false,
		"controller":         s.autoController.Status(),
	})
}
