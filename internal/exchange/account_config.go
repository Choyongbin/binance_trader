package exchange

import (
	"context"
	"errors"
	"net/url"
	"time"
)

// AccountCapability contains account-wide capability/configuration fields that
// Binance removed from /fapi/v3/account. These values must be read from the
// dedicated /fapi/v1/accountConfig endpoint instead of inferred from missing
// JSON fields in V3 responses.
type AccountCapability struct {
	CanTrade          bool
	MultiAssetsMargin bool
	DualSidePosition  bool
}

func (c *Client) fetchAccountCapability(ctx context.Context, key, secret string, offset time.Duration) (AccountCapability, error) {
	var wire struct {
		CanTrade          *bool `json:"canTrade"`
		MultiAssetsMargin *bool `json:"multiAssetsMargin"`
		DualSidePosition  *bool `json:"dualSidePosition"`
	}
	if err := c.signedGet(ctx, "/fapi/v1/accountConfig", url.Values{}, key, secret, offset, &wire); err != nil {
		return AccountCapability{}, err
	}
	if wire.CanTrade == nil || wire.MultiAssetsMargin == nil || wire.DualSidePosition == nil {
		return AccountCapability{}, errors.New("Demo 계정 설정 응답에 거래 가능/멀티자산/포지션 모드 필드가 누락되었습니다")
	}
	return AccountCapability{
		CanTrade:          *wire.CanTrade,
		MultiAssetsMargin: *wire.MultiAssetsMargin,
		DualSidePosition:  *wire.DualSidePosition,
	}, nil
}

// fetchAccountPortfolio reads the V3 portfolio plus the account-wide fields
// that were moved to /fapi/v1/accountConfig. Keeping the merge here prevents a
// missing V3 JSON field from silently becoming Go's false zero value.
func (c *Client) fetchAccountPortfolio(ctx context.Context, key, secret string, offset time.Duration) (AccountPortfolio, AccountCapability, error) {
	var p AccountPortfolio
	if err := c.signedGet(ctx, "/fapi/v3/account", url.Values{}, key, secret, offset, &p); err != nil {
		return AccountPortfolio{}, AccountCapability{}, err
	}
	if err := validPortfolio(p); err != nil {
		return AccountPortfolio{}, AccountCapability{}, err
	}
	cap, err := c.fetchAccountCapability(ctx, key, secret, offset)
	if err != nil {
		return AccountPortfolio{}, AccountCapability{}, err
	}
	p.CanTrade = cap.CanTrade
	p.MultiAssetsMargin = cap.MultiAssetsMargin
	return p, cap, nil
}
