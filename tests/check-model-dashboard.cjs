'use strict';
// Headless DOM contract for read-only model evidence. No browser/private API.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const src=fs.readFileSync('internal/server/web/app.js','utf8');
const nodes=new Map();
function el(tag='div'){
  const e={tag,children:[],dataset:{},className:'',textContent:'',value:'',checked:false,
    classList:{add(){},remove(){},toggle(){}},setAttribute(){},addEventListener(){},
    append(...items){this.children.push(...items)},replaceChildren(){this.children=[]},
    querySelectorAll(){return []}};
  Object.defineProperty(e,'childElementCount',{get(){return this.children.length}});
  return e;
}
const document={getElementById(id){if(!nodes.has(id))nodes.set(id,el());return nodes.get(id)},createElement:el,querySelectorAll(){return []}};
const sandbox={document,window:{addEventListener(){}},Date,Number,JSON,String,Set,Array,console,setTimeout(){return 0},clearTimeout(){}};
vm.createContext(sandbox);vm.runInContext(src,sandbox);
vm.runInContext('state.models=[{id:"m1",name:"테스트 모델",description:""}];state.selected=["m1"];renderModels()',sandbox);
assert.match(nodes.get('multiModelEvidenceStatus').textContent,/주문 잠금/);
assert.match(nodes.get('modelStatsBody').children[0].children[0].textContent,/조회 대기/);
vm.runInContext(`state.modelDashboard={selected_model_ids:['m1'],trading_authorized:false,models:[{model_id:'m1',selected:true,market_signal:'LONG',local_long_quantities:['0.001'],local_short_quantities:[],realized_pnl_status:'UNVERIFIED',unrealized_pnl_status:'UNVERIFIED'}],run_control:{state:'STOPPED'},controller:{running:false},risk_blockers:['미정산'],signed_account_fresh:false,accounting_report_count:1,active_reservations:0,lifecycle_pending:1,signed_long_qty:'0.001',signed_short_qty:'0',observed_at:new Date().toISOString(),local_snapshot_consistent:false};renderModelDashboard()`,sandbox);
const cols=nodes.get('modelStatsBody').children[0].children;
assert.match(cols[3].textContent,/0\.001 BTC.*로컬/);
assert.match(cols[4].textContent,/0 증명/);
assert.match(cols[5].textContent,/미검증/);
assert.match(cols[6].textContent,/미검증/);
assert.match(nodes.get('modelControlState').textContent,/STOPPED/);
assert.match(nodes.get('modelLifecycleState').textContent,/1건/);
// A selection update must never show old model/position data.
vm.runInContext('state.selected=[];renderModelDashboard()',sandbox);
assert.equal(nodes.get('modelStatsBody').children[0].children.length,1);
assert.match(nodes.get('modelBlockers').textContent,/423/);
vm.runInContext('state.environment="real";state.modelDashboard=null;renderModelDashboard()',sandbox);
assert.match(nodes.get('autoRunSummary').textContent,/주문 잠금/);
console.log('PASS read-only model dashboard DOM, fail-closed on stale data and environment changes');
