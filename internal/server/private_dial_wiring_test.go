package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Guards the exact production wiring that dev.3.2 got wrong. Both callback
// types are func(context.Context, string), but the string has different units:
// StreamDemoUserEvents sends a URL; the old function treated it as a key.
func TestProductionPrivateProbeUsesURLDialerNotKeyDialer(t *testing.T) {
	tree, err := parser.ParseFile(token.NewFileSet(), "server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	ast.Inspect(tree, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) != 5 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "ProbePrivateTransport" {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "trading" {
			return true
		}
		count++
		dialer, ok := call.Args[3].(*ast.SelectorExpr)
		if !ok || dialer.Sel.Name != "DialDemoUserStreamURL" {
			t.Error("production private probe must receive a URL-taking dialer")
			return true
		}
		packageName, ok := dialer.X.(*ast.Ident)
		if !ok || packageName.Name != "exchange" {
			t.Error("unexpected private probe dialer package")
		}
		return true
	})
	if count != 1 {
		t.Fatalf("expected one production private probe binding, found %d", count)
	}
}
