'use strict';
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync('internal/server/web/app.js','utf8');
const nodes=new Map();
function el(tag='div') {
 return {tag,children:[],textContent:'',className:'',disabled:false,
  replaceChildren(...els){this.children=els},append(...els){this.children.push(...els)},
  addEventListener(){},classList:{add(){},remove(){},toggle(){}}};
}
const document={getElementById(id){if(!nodes.has(id))nodes.set(id,el());return nodes.get(id)},createElement:el};
const sandbox={document,window:{addEventListener(){}},Date,Number,JSON,String,Set,Array,console};
vm.createContext(sandbox);
vm.runInContext(source,sandbox);
const banner=nodes.get('operationsOverviewBanner') || document.getElementById('operationsOverviewBanner');
const hostile='<img src=x onerror=alert(1)><script>attack()</script>';
sandbox.setOverviewBanner(banner,'error',hostile,hostile);
assert.equal(banner.children.length,2);
assert.equal(banner.children[0].tag,'b');
assert.equal(banner.children[0].textContent,hostile);
assert.equal(banner.children[1].tag,'span');
assert.equal(banner.children[1].textContent,hostile);
assert.equal(banner.className,'overview-banner error');
assert.doesNotMatch(source, /innerHTML\s*=/);
console.log('PASS overview banner renders untrusted status and errors as plain text');
