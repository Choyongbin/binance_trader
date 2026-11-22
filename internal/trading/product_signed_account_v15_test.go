package trading

import (
    "strings"
    "testing"
    "time"

    "binance_trader/internal/exchange"
)

func signedAccountFixtureV15() exchange.RecoveryBaseline {
    now:=time.Now().UTC()
    b:=recoveryBaseline(now)
    b.Portfolio.Positions=[]exchange.PortfolioPosition{
        {Symbol:"BTCUSDT",PositionSide:"LONG",PositionAmt:"0.01000000",InitialMargin:"1",Isolated:true},
        {Symbol:"BTCUSDT",PositionSide:"SHORT",PositionAmt:"-0.02000000",InitialMargin:"2",Isolated:true},
    }
    return b
}
func TestSignedAccountFrameV15MapsFullSignedAccountBothSides(t *testing.T){
    b:=signedAccountFixtureV15()
    got,err:=signedAccountFrameV15(b,strings.Repeat("a",64))
    if err!=nil || got.LongQtyBTC!="0.01" ||got.ShortQtyBTC!="0.02" ||
        got.AccountEvidenceSHA256=="" || got.ForeignExposure || got.UnverifiedCollateral {
        t.Fatalf("frame=%+v err=%v",got,err)
    }
    // REST response order is not an ownership/evidence semantic.
    b.Portfolio.Positions[0],b.Portfolio.Positions[1]=b.Portfolio.Positions[1],b.Portfolio.Positions[0]
    other,err:=signedAccountFrameV15(b,strings.Repeat("a",64))
    if err!=nil || other.AccountEvidenceSHA256!=got.AccountEvidenceSHA256 {t.Fatalf("noncanonical signed frame: %+v %v",other,err)}
}
func TestSignedAccountFrameV15RejectsBadExposureAndMissingSignedLists(t *testing.T){
    for _,tc:=range []struct{name string;modify func(*exchange.RecoveryBaseline);hardError bool}{
        {"duplicate side",func(b *exchange.RecoveryBaseline){b.Portfolio.Positions=append(b.Portfolio.Positions,b.Portfolio.Positions[0])},true},
        {"wrong short sign",func(b *exchange.RecoveryBaseline){b.Portfolio.Positions[1].PositionAmt="0.02"},true},
        {"foreign active position",func(b *exchange.RecoveryBaseline){b.Portfolio.Positions=append(b.Portfolio.Positions,exchange.PortfolioPosition{Symbol:"ETHUSDT",PositionSide:"LONG",PositionAmt:"1",InitialMargin:"1",Isolated:true})},false},
        {"missing open orders",func(b *exchange.RecoveryBaseline){b.OpenOrders=nil},true},
        {"nonusdt collateral",func(b *exchange.RecoveryBaseline){b.Portfolio.Assets=append(b.Portfolio.Assets,exchange.PortfolioAsset{Asset:"BTC",WalletBalance:"1",AvailableBalance:"1",MarginBalance:"1"})},false},
    }{
        t.Run(tc.name,func(t *testing.T){b:=signedAccountFixtureV15();tc.modify(&b)
            got,err:=signedAccountFrameV15(b,strings.Repeat("a",64))
            if tc.hardError && err==nil{t.Fatalf("should fail %+v",got)}
            if !tc.hardError && (err!=nil ||(!got.ForeignExposure && !got.UnverifiedCollateral)){t.Fatalf("foreign evidence ignored %+v %v",got,err)}
        })
    }
}
func TestSignedOwnerTotalsV15PooledAndUnknownFailClosed(t *testing.T){
    owners:=[]OwnerPosition{
        {Source:Model,ModelID:"model_a",Symbol:"BTCUSDT",Side:Long,Quantity:"0.010"},
        {Source:Model,ModelID:"model_b",Symbol:"BTCUSDT",Side:Long,Quantity:"0.02"},
        {Source:Manual,Symbol:"BTCUSDT",Side:Short,Quantity:"0.003"},
    }
    long,short,err:=signedOwnerTotalsV15(owners)
    if err!=nil || long!="0.03" || short!="0.003"{t.Fatalf("pooled %+v %s %s",err,long,short)}
    owners[1].ModelID="model_a"
    if _,_,err=signedOwnerTotalsV15(owners);err==nil{t.Fatal("duplicate model position owner accepted")}
    owners[1].ModelID="model_b"
    owners[1].Quantity="-0.02"
    if _,_,err=signedOwnerTotalsV15(owners);err==nil{t.Fatal("negative owner quantity accepted")}
}
