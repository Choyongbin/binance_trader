'use strict';
// Deterministic headless account rendering tests: bad signed-REST fields must
// not become positive/negative/zero balances or model-attributed PnL.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const src=fs.readFileSync('internal/server/web/app.js','utf8');
const nodes=new Map();
function node(tag='div'){
 const n={tag,children:[],dataset:{},value:'',textContent:'',disabled:false,
 classList:{values:new Set(),toggle(k,on){if(on)this.values.add(k);else this.values.delete(k)},add(k){this.values.add(k)},remove(k){this.values.delete(k)}},
 append(...xs){this.children.push(...xs)},replaceChildren(){this.children=[]},setAttribute(){},addEventListener(){}};
 Object.defineProperty(n,'childElementCount',{get(){return this.children.length}});
 return n;
}
const document={getElementById(id){if(!nodes.has(id))nodes.set(id,node());return nodes.get(id)},createElement:node,querySelectorAll(){return []}};
const ctx={document,window:{addEventListener(){}},Date,Number,JSON,String,Set,Array,console,setTimeout(){},clearTimeout(){}};
vm.createContext(ctx);vm.runInContext(src,ctx);
function render(profits){
 const snapshot={status:'connected',as_of:'2026-10-23T06:00:00Z',balances:[{asset:'USDT',balance:'5000',availableBalance:'5000'}],
 positions: profits.map(p=>p===undefined?{symbol:'BTCUSDT',positionSide:'LONG',positionAmt:'0.001'}:
 {symbol:'BTCUSDT',positionSide:'LONG',positionAmt:'0.001',unRealizedProfit:p}),orders:[],algo_orders:[]};
 ctx.__sample=snapshot;vm.runInContext('renderAccount(__sample)',ctx);
 return nodes.get('unrealizedPnl').textContent;
}
assert.match(render(['12.375','-2.00']),/10[.,]375/);
assert.equal(render([]),'0');
for(const invalid of [undefined,null,'','  ','bogus','NaN','Infinity','-Infinity',{},true,0,'1,234','1e9999']){
 assert.match(render(['3.25',invalid]),/미확인/,`invalid aggregate incorrectly rendered zero: ${String(invalid)}`);
 assert.match(nodes.get('positionsBody').children[1].children[6].textContent,/미확인/);
 assert.equal(nodes.get('unrealizedPnl').classList.values.has('positive'),false);
 assert.equal(nodes.get('unrealizedPnl').classList.values.has('negative'),false);
}
ctx.__sample={status:'connected',as_of:'2026-10-23T06:00:00Z',balances:[]};
vm.runInContext('renderAccount(__sample)',ctx);
assert.match(nodes.get('unrealizedPnl').textContent,/미확인/,'missing positions array cannot imply flat/zero PnL');
assert.equal(nodes.get('positionCount').textContent,'—');
assert.equal(nodes.get('orderTabCount').textContent,'—');
assert.match(nodes.get('positionsBody').children[0].children[0].textContent,/확인할 수 없습니다/);
assert.match(nodes.get('ordersBody').children[0].children[0].textContent,/불완전/);
console.log('PASS strict account/unrealized PnL unknown propagation and malformed values');
