package wallet

import (
	"errors"
	"math/big"
	"regexp"
)

var riskAmount = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,12})(?:\.[0-9]{1,18})?$`)

func riskDecimal(s string) (*big.Rat, error) {
	if !riskAmount.MatchString(s) {
		return nil, errors.New("잘못된 내부 증거금 숫자")
	}
	v, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, errors.New("잘못된 내부 증거금 숫자")
	}
	return v, nil
}

// Assessment is an exact arithmetic budget preview. It is NOT a signed
// exchange snapshot, reconciled ownership report, or trade authorization.
// Callers must supply independently authenticated account values and full
// reconciled auto-owned/auto-reserved margins, never UI form values.
type Assessment struct {
	AssignedUSDT         string `json:"assigned_usdt"`
	OwnedUSDT            string `json:"owned_usdt"`
	PendingUSDT          string `json:"pending_usdt"`
	FreeBudgetUSDT       string `json:"free_budget_usdt"`
	AccountAvailableUSDT string `json:"account_available_usdt"`
	MaxNewModelEntryUSDT string `json:"max_new_model_entry_usdt"`
	Fits                 bool   `json:"fits"`
	Reason               string `json:"reason"`
}

func Assess(assigned, owned, pending, proposed, accountAvailable string) (Assessment, error) {
	values := []string{assigned, owned, pending, proposed, accountAvailable}
	rats := make([]*big.Rat, 0, len(values))
	for _, s := range values {
		v, e := riskDecimal(s)
		if e != nil {
			return Assessment{}, errors.New("예산 계산에 잘못된 USDT 금액")
		}
		rats = append(rats, v)
	}
	balance, used, reserved, add, actual := rats[0], rats[1], rats[2], rats[3], rats[4]
	free := new(big.Rat).Sub(balance, new(big.Rat).Add(used, reserved))
	out := Assessment{AssignedUSDT: Format(balance), OwnedUSDT: Format(used), PendingUSDT: Format(reserved), AccountAvailableUSDT: Format(actual)}
	if free.Sign() < 0 {
		out.Reason = "자동매매 배정액보다 이미 사용한 증거금이 큽니다"
		out.FreeBudgetUSDT = "0"
		out.MaxNewModelEntryUSDT = "0"
		return out, nil
	}
	out.FreeBudgetUSDT = Format(free)
	// Existing pending orders are already deducted from free; a conservative
	// additional check uses live exchange available. Never count equity twice.
	budget := new(big.Rat).Set(free)
	if actual.Cmp(budget) < 0 {
		budget.Set(actual)
	}
	max := new(big.Rat).Mul(budget, big.NewRat(1, 10))
	out.MaxNewModelEntryUSDT = Format(max)
	if balance.Sign() == 0 {
		out.Reason = "배정된 자동매매 자금이 없습니다"
	} else if add.Cmp(max) > 0 {
		out.Reason = "신규 모델 진입은 남은 지갑 예산과 가용 USDT 중 작은 금액의 10% 이하여야 합니다"
	} else if add.Cmp(free) > 0 || add.Cmp(actual) > 0 {
		out.Reason = "배정 예산 또는 실제 거래소 가용 USDT 부족"
	} else {
		out.Fits = true
	}
	return out, nil
}
