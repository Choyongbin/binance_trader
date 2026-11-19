package exchange

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"
)

// ExecutionPreflightSnapshot is a short-lived, read-only bracket of signed
// Demo account evidence intended for the final execution boundary. It is not
// an authorization token and must never be persisted as one.
type ExecutionPreflightSnapshot struct {
	Environment   string               `json:"environment"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    time.Time            `json:"finished_at"`
	Portfolio     AccountPortfolio     `json:"portfolio"`
	OpenOrders    []Order              `json:"open_orders"`
	OpenAlgos     []AlgoOrder          `json:"open_algos"`
	Configuration TradingConfiguration `json:"configuration"`
}

// FetchDemoExecutionPreflight brackets the order boundary with two independent
// signed account/order reads. Any mutation observed while the preflight is in
// progress fails closed. No POST/DELETE endpoint is called here.
func (c *Client) FetchDemoExecutionPreflight(ctx context.Context, key, secret, symbol string) (ExecutionPreflightSnapshot, error) {
	if err := c.demoOnly(); err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	if strings.TrimSpace(key) == "" || secret == "" {
		return ExecutionPreflightSnapshot{}, errors.New("Demo 인증 정보가 필요합니다")
	}
	if symbol != "BTCUSDT" {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight는 BTCUSDT만 지원합니다")
	}
	started := c.now().UTC()
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	get := func(path string, values url.Values, out any) error {
		return c.signedGet(ctx, path, values, key, secret, offset, out)
	}

	first, firstCapability, err := c.fetchAccountPortfolio(ctx, key, secret, offset)
	if err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	var firstOrders []Order
	if err = get("/fapi/v1/openOrders", url.Values{}, &firstOrders); err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	var firstAlgos []AlgoOrder
	if err = get("/fapi/v1/openAlgoOrders", url.Values{}, &firstAlgos); err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	if firstOrders == nil || firstAlgos == nil {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight 미체결 주문 응답 누락")
	}

	config, err := c.InspectTradingConfiguration(ctx, key, secret, symbol)
	if err != nil {
		return ExecutionPreflightSnapshot{}, err
	}

	last, lastCapability, err := c.fetchAccountPortfolio(ctx, key, secret, offset)
	if err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	var lastOrders []Order
	if err = get("/fapi/v1/openOrders", url.Values{}, &lastOrders); err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	var lastAlgos []AlgoOrder
	if err = get("/fapi/v1/openAlgoOrders", url.Values{}, &lastAlgos); err != nil {
		return ExecutionPreflightSnapshot{}, err
	}
	if lastOrders == nil || lastAlgos == nil {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight 최종 주문 응답 누락")
	}
	if firstCapability != lastCapability || !sameAccountBoundary(first, last) || !stableOrders(firstOrders, lastOrders) || !stableAlgos(firstAlgos, lastAlgos) {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight 조회 중 계정/포지션/주문 상태가 변경되었습니다")
	}
	finished := c.now().UTC()
	if finished.Before(started) || finished.Sub(started) > 5*time.Second {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight가 freshness 한도를 초과했습니다")
	}
	return ExecutionPreflightSnapshot{
		Environment: "demo", StartedAt: started, FinishedAt: finished,
		Portfolio: last, OpenOrders: lastOrders, OpenAlgos: lastAlgos, Configuration: config,
	}, nil
}
