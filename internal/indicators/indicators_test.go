package indicators

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

func TestJSParityAndInputRejection(t *testing.T) {
	raw, err := os.ReadFile("testdata/parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Close    []float64 `json:"close"`
		Expected Series    `json:"expected"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	actual, err := Calculate(fixture.Close)
	if err != nil {
		t.Fatal(err)
	}
	compares := []struct {
		field            string
		actual, expected []*float64
	}{
		{"ema7", actual.EMA7, fixture.Expected.EMA7}, {"ema25", actual.EMA25, fixture.Expected.EMA25}, {"ema99", actual.EMA99, fixture.Expected.EMA99}, {"sma25", actual.SMA25, fixture.Expected.SMA25}, {"sma99", actual.SMA99, fixture.Expected.SMA99}, {"rsi", actual.RSI, fixture.Expected.RSI}, {"macd", actual.MACD, fixture.Expected.MACD}, {"signal", actual.Signal, fixture.Expected.Signal}, {"hist", actual.Histogram, fixture.Expected.Histogram}}
	for _, cmp := range compares {
		if len(cmp.actual) != len(cmp.expected) {
			t.Fatalf("%s count mismatch", cmp.field)
		}
		for i := range cmp.actual {
			a, b := cmp.actual[i], cmp.expected[i]
			if (a == nil) != (b == nil) {
				t.Fatalf("%s warmup mismatch at %d", cmp.field, i)
			}
			if a != nil && math.Abs(*a-*b) > 1e-7 {
				t.Fatalf("%s mismatch at %d got %g expected %g", cmp.field, i, *a, *b)
			}
		}
	}
	for _, invalid := range [][]float64{nil, {}, {1, math.NaN()}, {math.Inf(1)}} {
		if _, e := Calculate(invalid); e == nil {
			t.Fatalf("invalid input accepted %v", invalid)
		}
	}
}
func TestRSIConstant(t *testing.T) {
	c := make([]float64, 100)
	for i := range c {
		c[i] = 100
	}
	x, _ := Calculate(c)
	if x.RSI[13] != nil || x.RSI[14] == nil || *x.RSI[14] != 50 {
		t.Fatalf("bad Wilder RSI init")
	}
	if x.EMA99[98] == nil || x.EMA99[97] != nil {
		t.Fatal("bad EMA99 warmup")
	}
}
