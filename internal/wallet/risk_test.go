package wallet

import "testing"

func TestBudget(t *testing.T) {
	r, e := Assess("100", "20", "10", "7", "500")
	if e != nil || !r.Fits || r.FreeBudgetUSDT != "70" || r.MaxNewModelEntryUSDT != "7" {
		t.Fatal(r, e)
	}
	r, e = Assess("100", "20", "10", "7.00000001", "500")
	if e != nil || r.Fits {
		t.Fatal(r, e)
	}
	r, e = Assess("100", "20", "10", "0.5", "3")
	if e != nil || r.Fits {
		t.Fatal(r, e)
	}
	r, e = Assess("100", "120", "0", "0", "500")
	if e != nil || r.Fits {
		t.Fatal(r, e)
	}
	r, e = Assess("0", "0", "0", "0", "500")
	if e != nil || r.Fits {
		t.Fatal(r, e)
	}
}
