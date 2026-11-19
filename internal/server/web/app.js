'use strict';

// Phase 2.6: Go receives Binance Demo public 1m klines over WebSocket.
// The UI polls the local read-only snapshot every 250ms to match the public
// kline stream cadence more closely. Private account data remains REST-updated
// independently. No trading methods are called.
const MARKET_POLL_INTERVAL_MS = 250;
const state = {
  csrf: '', environment: 'demo', generation: 0, account: null,
  models: [], selected: [], market: null, indicators: null,
  enabledOverlays: new Set(['ema7', 'ema25', 'ema99', 'sma25', 'sma99']),
  orderType: 'market', tab: 'positions', marketBusy: false, marketFailureKey: '', accountBusy: false,
  events: [], walletInfo:null, walletBusy:false, modelSignalBusy:false, modelEvidenceBusy:false, modelDashboard:null, modelDashboardError:"", manualPreviewGeneration:0, accountingOffset:0, accountingBusy:false, accountingTotal:0,
};
const $ = id => document.getElementById(id);
const num = (v, decimals=2) => {
  if (v === null || v === undefined || v === '' || !Number.isFinite(Number(v))) return '—';
  return Number(v).toLocaleString('ko-KR', { minimumFractionDigits: 0, maximumFractionDigits: decimals });
};
const signed = (v, decimals=3) => Number.isFinite(v) ? `${v > 0 ? '+' : ''}${num(v,decimals)}` : '—';
// Binance account PnL is a decimal string. Missing, malformed, non-finite
// or non-string values are UNKNOWN, never silently converted to zero.
// This is display-only: never use JS floats for settlement/accounting.
const parseAccountPnl = raw => {
  if (typeof raw !== 'string' || !/^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$/.test(raw)) return null;
  const value=Number(raw);
  return Number.isFinite(value) ? value : null;
};
const KST_FORMAT = { timeZone:'Asia/Seoul', hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit' };
const kstTime = value => {
  const d = value instanceof Date ? value : new Date(value);
  return Number.isFinite(d.getTime()) ? d.toLocaleTimeString('ko-KR', KST_FORMAT) : '—';
};
let toastTimer;
function notify(message, isError = false) {
  const t = $('toast'); t.textContent = message;
  t.classList.toggle('error', isError); t.classList.add('visible');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('visible'), 4000);
}
function record(message) {
  state.events.unshift({ at: new Date(), message });
  state.events.length = Math.min(state.events.length, 80);
  const root = $('logsBody'); root.replaceChildren();
  for (const entry of state.events) {
    const p = document.createElement('p');
    p.textContent = `${entry.at.toLocaleTimeString('ko-KR')} · ${entry.message}`;
    root.append(p);
  }
}
async function request(path, opts = {}) {
  const method = opts.method || 'GET';
  const headers = { Accept: 'application/json' };
  if (method !== 'GET') {
    headers['Content-Type'] = 'application/json';
    headers['X-CSRF-Token'] = state.csrf;
  }
  let response;
  try { response = await fetch(path, { ...opts, method, headers, cache:'no-store', credentials:'same-origin' }); }
  catch { throw new Error('로컬 서버에 연결할 수 없습니다'); }
  let data;
  try { data = await response.json(); } catch { throw new Error('서버 응답을 읽을 수 없습니다'); }
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}
// /market/live deliberately returns the current WebSocket status body with 503
// while the stream is not ready. Keep that status instead of turning it into a
// generic exception so the 250ms market loop needs only this single endpoint.
async function requestMarketLive() {
  let response;
  try { response = await fetch('/api/v1/market/live', { headers:{ Accept:'application/json' }, cache:'no-store', credentials:'same-origin' }); }
  catch { throw new Error('로컬 서버에 연결할 수 없습니다'); }
  let data;
  try { data = await response.json(); } catch { throw new Error('서버 응답을 읽을 수 없습니다'); }
  if (response.status === 503) return { ready:false, data };
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return { ready:true, data };
}
function markDot(id, status) {
  const el = $(id); el.classList.toggle('good', status === 'connected');
  el.classList.toggle('bad', status === 'error');
}
function clock() {
  $('headerClock').textContent = new Date().toLocaleTimeString('ko-KR', { hour12: false });
}
function applyEnvironment(env) {
  state.environment = env; state.generation++; state.manualPreviewGeneration++;
  $('manualPreviewResult').textContent='Demo 계정 연결 및 실시간 시세 수신 후 서버 사전점검을 실행할 수 있습니다.';
  $('demoConfigSection').classList.toggle('hidden', env !== 'demo');
  $('demoPrivateSection').classList.toggle('hidden', env !== 'demo');
  $('demoAccountingSection').classList.toggle('hidden', env !== 'demo');
  $('demoRecoverySection').classList.toggle('hidden', env !== 'demo');
  $('demoMonitorSection').classList.toggle('hidden', env !== 'demo');
  $('demoConfigResult').textContent='데모 계정 연결 후 수동으로 실제 거래소 설정을 조회할 수 있습니다.';
  $('demoBtn').classList.toggle('selected', env === 'demo');
  $('realBtn').classList.toggle('real-selected', env === 'real');
  $('demoCredentialSection').classList.toggle('hidden', env !== 'demo');
  $('realLockInfo').classList.toggle('hidden', env !== 'real');
  state.market = null; state.indicators = null; state.marketFailureKey = '';state.walletInfo=null;state.modelDashboard=null;state.modelDashboardError="";renderWallet();renderModelDashboard();
  $('marketStatus').textContent = env === 'real' ? '실거래 시세 잠금' : '데모 WS 연결 대기';
  $('marketAsOf').textContent = '실제 수신 데이터 없음';
  $('chartEmptyTitle').textContent = env === 'real' ? '실거래 시세는 아직 지원하지 않습니다' : '데모 WebSocket 데이터 수신 대기';
  $('chartEmptyDetail').textContent = env === 'real' ? '실거래 조회 및 주문은 잠겨 있습니다.' : '데모 공개 시세 수신 후 실제 캔들을 표시합니다.';
  markDot('marketDot','locked');
  renderMarket();
  renderAccount({ status: env === 'real' ? 'real_locked' : 'locked' });
}
async function switchEnvironment(env) {
  if (env === state.environment) return;
  if (env === 'real' && !confirm('실거래 화면으로 전환하면 데모 연결이 종료됩니다. 이번 버전에서는 실거래 연결과 주문이 모두 잠겨 있습니다. 계속할까요?')) return;
  try {
    const result = await request('/api/v1/settings/environment', { method: 'POST', body: JSON.stringify({ environment:env }) });
    applyEnvironment(result.environment);
    record(env === 'real' ? '실거래 잠금 화면으로 전환' : '데모 조회 화면으로 전환');
    await Promise.all([syncAccount(), syncMarket()]);
    notify(env === 'real' ? '실거래는 잠겨 있습니다.' : '데모 환경으로 전환했습니다. 암호로 다시 연결하세요.');
  } catch(e) { notify(e.message, true); }
}
function showSettings() { $('settingsDialog').showModal(); }
async function inspectDemoConfig() {
  if (state.environment!=='demo' || state.account?.status!=='connected') {
    $('demoConfigResult').textContent='데모 계정을 먼저 연결해 주세요.';return;
  }
  const btn=$('inspectConfigBtn'), gen=state.generation;
  btn.disabled=true;$('demoConfigResult').textContent='Binance Demo 거래소 설정 확인 중…';
  try {
    const conf=await request('/api/v1/trading/config');
    if(gen!==state.generation || state.environment!=='demo')return;
    const mode=conf.hedge_mode?'양방향 (Hedge)':'단방향 (One-way)';
    const margin=conf.margin_type==='ISOLATED'?'격리':'교차';
    const details=`포지션: ${mode} · 증거금: ${margin} · 레버리지: ${conf.leverage}배 · 자동 증거금 추가: ${conf.auto_add_margin?'ON':'OFF'}`;
    const summary=conf.settings_match_v1?'설정 일치 (실제 주문 기능은 아직 잠금 상태)':'설정 불일치 · 실제 거래소 설정을 확인해야 합니다';
    const warnings=(conf.warnings||[]).join(' / ');
    $('demoConfigResult').textContent=[summary,details,warnings,`조회: ${new Date(conf.as_of).toLocaleTimeString('ko-KR')}`].filter(Boolean).join('\n');
    record('Demo 거래소 포지션·증거금·레버리지 조회 완료');
  } catch(e) {if(gen===state.generation){$('demoConfigResult').textContent=`설정 조회 실패: ${e.message}`;}}
  finally {btn.disabled=false;}
}
const privateProbeLabels={HANDSHAKE_ONLY:'접속 성공 · 이벤트 수신 없음 (정상일 수 있음)',EVENTS_OBSERVED:'접속 성공 · 계정 이벤트 일부 관측',LISTEN_KEY_FAILED:'Demo listenKey 발급·확인 실패',WS_HANDSHAKE_FAILED:'비공개 WS 접속 실패',WS_HANDSHAKE_TIMEOUT:'비공개 WS 연결 제한시간 초과',STREAM_INTERRUPTED_OR_INVALID_EVENT:'관측 중 스트림 중단 또는 예상 외 이벤트',OBSERVER_NOT_STOPPED:'관측기 종료 확인 실패 · 수동 조사 필요',CANCELLED:'진단 요청 중단',SESSION_CHANGED:'진단 중 Demo 계정 인증 상태 변경',INVALID_CONFIGURATION:'진단 설정 오류'};
function privateSummary(report) {
  const label=privateProbeLabels[report.status]||'진단 상태 확인 필요';
  const summary=`${label} · 핸드셰이크: ${report.handshake_ok?'성공':'미확인'} · 이벤트: ${report.event_count||0}건`;
  const phase=report.dial_phase?`연결 방식: ${report.dial_route||"이전 기록"} · 연결 단계: ${report.dial_phase}${report.dial_http_status?' · HTTP '+report.dial_http_status:''}${Number.isInteger(report.dial_elapsed_ms)?' · '+report.dial_elapsed_ms+'ms':''}`:'연결 단계 진단: 이전 기록에는 없음';
  return `${summary}\n${phase}\n${report.event_types?.join(', ')||'관측 이벤트 유형 없음'}\n※ 이 기록은 과거 접속 진단만 의미하며, 자동매매·수동 주문을 허용하지 않습니다.`;
}
async function inspectPrivateHistory(){
 if(state.environment!=='demo'){return;}
 try {const history=await request('/api/v1/trading/private-probe/status');
  $('privateProbeResult').textContent=history.status==='NOT_RUN'?'저장된 비공개 스트림 진단이 없습니다.':`과거 진단 ${history.report_count}회 · ${privateSummary(history)}`;
 }catch(e){$('privateProbeResult').textContent=`기록 조회 실패: ${e.message}`;}
}
async function inspectPrivateProbe(e){
 e.preventDefault();
 const button=$('privateProbeRun'), form=$('privateProbeForm');
 if(state.environment!=='demo'||state.account?.status!=='connected'){form.reset();$('privateProbeResult').textContent='Demo 계정을 먼저 연결해 주세요.';return;}
 if(!confirm('5초 동안 Demo 비공개 계정 이벤트의 연결만 확인합니다. 실제 주문은 실행하지 않습니다. 진행할까요?')){form.reset();return;}
 const generation=state.generation;
 const passphrase=$('privateProbePassphrase').value;
 form.reset();button.disabled=true;$('privateProbeResult').textContent='Demo 사설 WebSocket 접속 진단 중… (최대 약 20초)';
 try{
  const r=await request('/api/v1/trading/private-probe/run',{method:'POST',body:JSON.stringify({passphrase})});
  if(state.generation===generation&&state.environment==='demo')$('privateProbeResult').textContent=privateSummary(r);
  record('Demo 사설 스트림 단기 진단 수행 (읽기 전용)');
 }catch(e){if(state.generation===generation)$('privateProbeResult').textContent=`진단 실패: ${e.message}`;}
 finally{button.disabled=false;}
}
// A live transport status is NOT a trading permit. All values come from
// sanitized local monitor checkpoints; never display private WS URLs or keys.
async function inspectContinuousMonitor(){
 if(state.environment!=='demo')return;
 try{
   const r=await request('/api/v1/recovery/continuous-monitor/status');
   if(r.trading_authorized!==false||typeof r.stream_continuity_proven!=='boolean'||r.manual_review_required!==true)throw new Error('안전 상태 플래그 검사 실패');
   const last=r.last_signed_rest_at?new Date(r.last_signed_rest_at).toLocaleString('ko-KR'):'최근 서명 REST 없음';
   $('monitorResult').textContent=`${r.state} · 동작 중: ${r.running?'예':'아니오'} · Epoch: ${r.epoch||0} · 재연결: ${r.reconnects||0}회\n최근 서명 REST: ${last}\n현재 프로세스 checkpoint 이후 연속성: ${r.stream_continuity_proven===true?'확인됨':'미확인'}${r.continuity_since?' · since '+new Date(r.continuity_since).toLocaleString('ko-KR'):''}\n검토 사항: ${(r.issues||[]).slice(0,5).join(' / ')||'계정 연속성 및 거래 안전성 미검증'}${r.stale_or_interrupted?'\n이전 프로세스의 미종료 기록: 수동 조사 필요':''}\n※ checkpoint는 과거 전체 이력 증명이나 거래 허가가 아니며 모든 주문은 잠금 상태입니다.`;
 }catch(e){$('monitorResult').textContent=`관측 기록 확인 불가 · 거래 잠금 유지: ${e.message}`;}
}
async function startContinuousMonitor(event){
 event.preventDefault();
 const form=$('demoMonitorForm');
 if(state.environment!=='demo'||state.account?.status!=='connected'){form.reset();notify('먼저 Demo 계정에 연결해 주세요.',true);return;}
 if(!confirm('서버가 실행되는 동안 읽기 전용 비공개 WS와 5분마다 서명 REST를 사용합니다. 재시작 시 자동 재연결하지 않으며 주문은 계속 잠깁니다. 진행할까요?')){form.reset();return;}
 const passphrase=$('monitorPassphrase').value,lookback_hours=Number($('monitorLookback').value);
 form.reset();$('monitorStart').disabled=true;
 try{await request('/api/v1/recovery/continuous-monitor/start',{method:'POST',body:JSON.stringify({passphrase,lookback_hours})});
  record('운영자 승인 읽기 전용 계정 관측 시작'); await inspectContinuousMonitor();
 }catch(e){notify(e.message,true);await inspectContinuousMonitor();}
 finally{$('monitorStart').disabled=false;}
}
async function stopContinuousMonitor(){
 if(!confirm('상시 읽기 전용 관측을 종료할까요? 종료 후 마지막 계정 상태는 더 이상 최신이 아닙니다.'))return;
 $('monitorStop').disabled=true;
 try{await request('/api/v1/recovery/continuous-monitor/stop',{method:'POST',body:'{}'});record('상시 계정 관측 수동 종료');await inspectContinuousMonitor();}
 catch(e){notify(e.message,true);await inspectContinuousMonitor();}
 finally{$('monitorStop').disabled=false;}
}
async function generateBoundedAccounting(e){
 e.preventDefault();
 const form=$('demoAccountingForm'), btn=$('accountingGenerate');
 if(state.environment!=='demo' || state.account?.status!=='connected'){
   form.reset();$('accountingGenerateResult').textContent='먼저 실제 Demo 계정을 연결해 주세요.';return;
 }
 if(!confirm('Demo에서 서명된 과거 계정 이력만 조회하고 로컬 체결과 비교합니다. 실제 주문은 실행하지 않습니다. 진행할까요?')){
   form.reset();return;
 }
 const passphrase=$('accountingPassphrase').value;
 const lookback_hours=Number($('accountingLookback').value), gen=state.generation;
 form.reset();btn.disabled=true;
 $('accountingGenerateResult').textContent='서명된 Demo 이력 조회 및 소유 장부 대조 중… 최대 약 80초';
 try {
   const report=await request('/api/v1/trading/accounting/generate',{
     method:'POST',body:JSON.stringify({passphrase,lookback_hours})
   });
   if(gen!==state.generation || state.environment!=='demo')return;
   $('accountingGenerateResult').textContent=`과거 회계 보고서 저장 완료 · 소유 거래 ${report.known_orders}건 · 체결 ${report.known_fills}건 · 소득 기록 ${report.income_rows}건. SQLite 색인에 반영하려면 운영자 report-index CLI를 실행해 주세요. 이 기록은 최종 순손익이 아닙니다.`;
   record('서명된 Demo 과거 회계 보고서 생성 (매매 기능 미사용)');
 } catch(err) {
   if(gen===state.generation)$('accountingGenerateResult').textContent=`보고서 생성 중단: ${err.message}`;
 } finally{btn.disabled=false;}
}

function setOverviewValue(id,value,detail){
 const valueEl=$(id), detailEl=$(`${id}Detail`);
 if(valueEl)valueEl.textContent=value;
 if(detailEl)detailEl.textContent=detail;
}
// Never interpolate server-provided status or errors into HTML markup.
// A localhost UI still treats API response strings as untrusted data.
function setOverviewBanner(banner, variant, title, detail){
 banner.className=`overview-banner ${variant}`;
 const heading=document.createElement('b');
 const description=document.createElement('span');
 heading.textContent=String(title);
 description.textContent=String(detail);
 banner.replaceChildren(heading,description);
}
async function inspectOperationsOverview(){
 const button=$('operationsOverviewRefresh'), banner=$('operationsOverviewBanner');
 if(!button||!banner)return;
 if(state.environment!=='demo'){
  setOverviewBanner(banner,'locked','실거래 잠금','Demo 운영 안전 요약만 지원합니다.');
  return;
 }
 button.disabled=true;
 setOverviewBanner(banner,'locked','거래 잠금 검증 중','로컬 안전 상태를 교차 조회하고 있습니다.');
 try{
  const paths=['/api/v1/trading/readiness','/api/v1/trading/auto/status','/api/v1/recovery/continuous-monitor/status','/api/v1/trading/operator-status','/api/v1/wallet/auto'];
  const results=await Promise.allSettled(paths.map(path=>request(path)));
  const [readyR,autoR,monitorR,operatorR,walletR]=results;
  const failures=results.filter(x=>x.status!=='fulfilled').length;
  const ready=readyR.status==='fulfilled'?readyR.value:null;
  const auto=autoR.status==='fulfilled'?autoR.value:null;
  const monitor=monitorR.status==='fulfilled'?monitorR.value:null;
  const operator=operatorR.status==='fulfilled'?operatorR.value:null;
  const wallet=walletR.status==='fulfilled'?walletR.value:null;

  // UI must never infer authorization from READY/continuity/no blockers.
  // Every authoritative endpoint that exposes an authorization bit must remain false,
  // and the auto routes must remain locked while this release is LOCKED.
  const unsafe=(ready&&ready.trading_authorized!==false)||
    (operator&&operator.trading_authorized!==false)||
    (monitor&&monitor.trading_authorized!==false)||
    (auto&&(auto.trading_enabled!==false||auto.start_route_locked!==true||auto.stop_route_locked!==true));
  if(unsafe)throw new Error('서버 안전 플래그 불일치: 거래 기능이 예상보다 열려 있습니다');

  setOverviewValue('overviewAuthorization','LOCKED',auto?`start ${auto.start_route_locked?'잠금':'열림'} · stop ${auto.stop_route_locked?'잠금':'열림'}`:'auto 상태 조회 실패');
  setOverviewValue('overviewReadiness',ready?(ready.ready?'PRECONDITIONS OK':'BLOCKED'):'UNKNOWN',ready?`${ready.state} · blocker ${(ready.blockers||[]).length}건`:'readiness 조회 실패');
  const ctl=auto?.control||{}, controller=auto?.controller||{};
  setOverviewValue('overviewAuto',auto?String(ctl.state||'UNKNOWN'):'UNKNOWN',auto?`generation ${ctl.generation??'—'} · controller ${controller.running?'RUNNING':'STOPPED'}${controller.fail_closed?' · FAIL-CLOSED':''}`:'auto 상태 조회 실패');
  setOverviewValue('overviewContinuity',monitor?(monitor.stream_continuity_proven?'CHECKPOINT OK':'UNPROVEN'):'UNKNOWN',monitor?`${monitor.state||'UNKNOWN'} · epoch ${monitor.epoch||0} · reconnect ${monitor.reconnects||0}`:'monitor 상태 조회 실패');
  setOverviewValue('overviewWallet',wallet?(wallet.wallet?.verified&&Number(wallet.wallet?.assigned_usdt)>0?'VERIFIED':'NOT READY'):'UNKNOWN',wallet?`assigned ${wallet.wallet?.assigned_usdt||'0'} USDT · signed audit ${wallet.requires_signed_audit?'필요':'상태 확인'}`:'wallet 조회 실패');
  setOverviewValue('overviewEvidence',operator?String(operator.state||'UNKNOWN'):'UNKNOWN',operator?`blocker ${(operator.blockers||[]).length}건 · gap ${operator.gap_marker_count||0}건`:'operator 증적 조회 실패');

  const readinessBlocked=!ready||ready.ready!==true;
  const evidenceBlocked=!operator||(operator.blockers||[]).length>0||operator.manual_review_required===true;
  const controllerRunning=controller.running===true;
  let cls='good', title='LOCKED · 로컬 사전조건 점검 가능', detail='거래는 잠겨 있으며 각 진단 결과는 독립적인 안전 증적입니다.';
  if(failures){cls='error';title='LOCKED · 일부 상태 조회 실패';detail=`${failures}개 진단을 읽지 못했습니다. 미확인 상태를 안전으로 간주하지 않습니다.`;}
  else if(readinessBlocked||evidenceBlocked||controllerRunning){cls='review';title='LOCKED · 검토 또는 차단 상태';detail=`readiness ${ready?.state||'UNKNOWN'} · operator ${operator?.state||'UNKNOWN'} · controller ${controllerRunning?'RUNNING':'STOPPED'}`;}
  setOverviewBanner(banner,cls,title,detail);
  $('operationsOverviewWarning').textContent='거래 허가: false. READY, continuity 확인, blocker 0건 중 어느 것도 단독으로 주문 허가를 의미하지 않습니다. 실제 side effect 직전 fresh signed preflight와 final guard가 다시 필요합니다.';
 }catch(err){
  setOverviewBanner(banner,'error','LOCKED · 상태 검증 실패',err.message);
  setOverviewValue('overviewAuthorization','LOCKED','검증 실패 · 자동 허가 없음');
 }finally{button.disabled=false;}
}

async function inspectTradingReadiness(){
 try{
  const r=await request('/api/v1/trading/readiness');
  if(r.trading_authorized!==false||r.fresh_preflight_required!==true||r.final_guard_required!==true)throw new Error('readiness 안전 플래그 검사 실패');
  const lines=[
   `readiness: ${r.state} · READY ${r.ready?'예':'아니오'}`,
   `market ${r.market_ready?'OK':'BLOCK'} · account ${r.account_ready?'OK':'BLOCK'} · wallet ${r.wallet_ready?'OK':'BLOCK'}`,
   `execution ${r.execution_ready?'OK':'BLOCK'} · protection ${r.protection_ready?'OK':'BLOCK'} · mutation ${r.mutation_ready?'OK':'BLOCK'}`,
   `차단 사유: ${(r.blockers||[]).join(' / ')||'없음'}`,
   '※ READY도 주문 허가 토큰이 아니며 실제 주문 직전 동일 gate를 다시 계산해야 합니다.'
  ];
  $('tradingReadinessResult').textContent=lines.join('\n');
 }catch(err){$('tradingReadinessResult').textContent=`readiness 조회 실패 · 거래 차단 유지: ${err.message}`;}
}

async function inspectOperatorStatus(){
 try{
   const r=await request('/api/v1/trading/operator-status');
   const p=r.protection||{}, a=r.accounting||{}, rec=r.recovery||{}, integ=r.integrated||{};
   const lines=[
     `상태: ${r.state}`,
     `재시작/스트림: ${r.restart_disposition} · monitor ${r.monitor_state||'NOT_RUN'} · gap ${r.gap_marker_count||0}건`,
     `복구: probe ${rec.probe_count||0}건 · interrupted ${rec.interrupted_count||0}건 · review ${rec.review_count||0}건`,
     `통합 검토: ${integ.latest_state||'NOT_RUN'} · 보고서 ${integ.report_count||0}건 · blocker ${integ.blocker_count||0}건`,
     `보호: ${p.arming_state||'EMPTY'} · plan ${p.plan_count||0} · settled ${p.settled_group_count||0} · unsettled ${p.unsettled_group_count||0} · fill ${p.protection_fill_count||0}`,
     `회계: 보고서 ${a.report_count||0}건 · 최근 known fills ${a.latest_known_fills||0} · protection fills ${a.latest_protection_fills||0} · dual-trigger ${a.latest_dual_triggered_groups||0}`,
     `차단 사유: ${(r.blockers&&r.blockers.length)?r.blockers.join(' / '):'없음(단, 거래 허가를 의미하지 않음)'}`,
     `※ trading_authorized=false · stream_continuity_proven=${r.stream_continuity_proven===true?'true (현재 프로세스 checkpoint 이후)':'false'} · evidence ${String(r.evidence_sha256||'').slice(0,16)}…`
   ];
   $('operatorStatusResult').textContent=lines.join('\n');
 }catch(err){ $('operatorStatusResult').textContent=`통합 운영 상태 조회 실패: ${err.message}`; }
}

// BOUNDED recovery, NOT continuous streaming and NEVER an order permit.

async function inspectOperatorRemediation(){
 try{
   const r=await request('/api/v1/trading/operator-remediation');
   const steps=Array.isArray(r.steps)?r.steps:[];
   const lines=[
     `복구 계획: ${r.state} · 최고 우선순위 ${r.highest_priority||'INFO'} · ${r.step_count||0}단계`,
     ...steps.map(s=>`${s.order}. [${s.priority}] ${s.title} · ${s.reason}\n   확인: ${(s.evidence_to_inspect||[]).join(' / ')}\n   조치: ${(s.operator_action||[]).join(' / ')}\n   금지: ${(s.forbidden_automation||[]).join(' / ')}`),
     `※ automatic_repair=false · mutation_performed=false · trading_authorized=false · evidence ${String(r.evidence_sha256||'').slice(0,16)}…`
   ];
   $('operatorRemediationResult').textContent=lines.join('\n');
 }catch(err){ $('operatorRemediationResult').textContent=`복구 가이드 조회 실패: ${err.message}`; }
}

async function inspectOperatorIncidentPackets(){
 try{
   const r=await request('/api/v1/trading/operator-incident-packets');
   const packets=Array.isArray(r.packets)?r.packets:[];
   const lines=[
     `CRITICAL incident packets: ${r.critical_packet_count||0}개 · operator ${r.source_operator_state||'UNKNOWN'}`,
     ...packets.map(p=>{
       const refs=Array.isArray(p.evidence_refs)?p.evidence_refs:[];
       return `[${p.code}] ${p.title} · packet ${String(p.packet_id||'').slice(0,16)}…
   refs: ${refs.map(x=>`${x.kind}:${x.id}:${x.state}:${String(x.sha256||'').slice(0,12)}…`).join(' / ')||'없음'}
   금지: ${(p.forbidden_automation||[]).join(' / ')}`;
     }),
     `※ 원본 증적 복사 없음 · automatic_repair=false · mutation_performed=false · trading_authorized=false · set ${String(r.sha256||'').slice(0,16)}…`
   ];
   $('operatorIncidentPacketsResult').textContent=lines.join('\n');
 }catch(err){ $('operatorIncidentPacketsResult').textContent=`incident packet 생성 실패: ${err.message}`; }
}

async function inspectOperatorIncidentTimeline(){
 try{
  const r=await request('/api/v1/trading/operator-incident-timeline'); const ev=Array.isArray(r.events)?r.events:[];
  const lines=[`incident timeline: ${r.event_count||0} events · ${r.source_operator_state||'UNKNOWN'}`, ...ev.map((x,i)=>`${i+1}. ${new Date(x.at).toLocaleString('ko-KR')} [${x.severity}] ${x.kind} · ${x.state} · ${x.summary}`), `현재 blocker: ${(r.active_blockers||[]).join(' / ')||'없음'}\n※ read-only · automatic_repair=false · mutation_performed=false · ${String(r.sha256||'').slice(0,16)}…`];
  $('operatorIncidentTimelineResult').textContent=lines.join('\n');
 }catch(err){$('operatorIncidentTimelineResult').textContent=`incident timeline 생성 실패: ${err.message}`;}
}

async function inspectRecoveryHistory() {
 if(state.environment!=='demo')return;
 try {
   const r=await request('/api/v1/recovery/status');
   const last=r.last_report;
   const summary=last?`${last.stage} · 비공개 연결: ${last.observer_connected?'확인':'미확인'} · 전체 REST: ${last.rest_reviewed?'조회':'미완료'} · ${new Date(last.finished_at||last.started_at).toLocaleString('ko-KR')}\n차단/주의: ${(last.blockers||[]).slice(0,4).join(' / ')||'과거 진단만 가능'}`:'기록된 복구 진단이 없습니다.';
   $('recoveryProbeResult').textContent=`과거 진단 ${r.report_count}건${r.diagnostic_running_or_stale_lock?' · 미종료 작업/잠금 수동 조사 필요':''}\n${summary}\n※ 거래 허가·연속 관측을 의미하지 않습니다.`;
 }catch(err){$('recoveryProbeResult').textContent=`복구 진단 기록 조회 실패: ${err.message}`;}
}
async function inspectRecoveryProbe(event) {
 event.preventDefault();
 const form=$('demoRecoveryForm'), btn=$('recoveryProbeRun'), result=$('recoveryProbeResult');
 if(state.environment!=='demo'||state.account?.status!=='connected'){
   form.reset();result.textContent='실제 Demo 계정 인증 연결을 먼저 완료해 주세요.';return;
 }
 if(!confirm('비공개 계정 WebSocket을 잠시 열고 서명된 전체 계정 REST 이력과 함께 비교합니다. 기존 미완료 작업은 수동 확인이 필요할 수 있습니다. 실제 주문은 전혀 실행하지 않습니다. 진행할까요?')){
   form.reset();return;
 }
 const passphrase=$('recoveryPassphrase').value;
 const lookback_hours=Number($('recoveryLookback').value), gen=state.generation;
 form.reset();btn.disabled=true;
 result.textContent='Demo 비공개 계정 WS·서명 전체 REST 조회 중… 최대 110초 (관측 후 자동 종료)';
 try {
   const review=await request('/api/v1/recovery/probe', {method:'POST',body:JSON.stringify({passphrase,lookback_hours})});
   if(gen!==state.generation || state.environment!=='demo')return;
   const blockers=(review.blockers||[]).slice(0,6).join(' / ');
   result.textContent=`${review.stage} · 계정 WS ${review.observer_connected?'접속 확인':'미확인'} · 서명 REST ${review.rest_reviewed?'검토 완료':'미완료'} · 이벤트 ${review.event_count}건 · 불연속 증적 ${review.discontinuity_count}건\n${blockers}\n과거 단기 관측만 완료. 주문 잠금 유지 / 스트림 연속성 미증명.`;
   record('Demo WS + REST 복구 진단 수행 (과거 기록, 거래 잠금)');
 } catch(err) {if(gen===state.generation)result.textContent=`복구 진단 중단: ${err.message}\n기존 복구 기록의 수동 검토가 필요할 수 있습니다.`;}
 finally {btn.disabled=false;}
}
function updateCredentialList(items) {
  const root = $('credentialList'); root.replaceChildren();
  for (const item of items || []) {
    const row = document.createElement('div');row.className='credential-row';
    const text = document.createElement('div'); const name=document.createElement('strong');name.textContent=item.label;
    const file=document.createElement('small');file.textContent=item.name;
    text.append(name,file);const status=document.createElement('span');status.textContent=item.state;
    row.append(text,status);root.append(row);
  }
}
async function credentialPost(path, body, success) {
  try {
    const result=await request(path, { method:'POST', body: JSON.stringify(body) });
    if (result.credentials) updateCredentialList(result.credentials);
    notify(success);record(success);
  } catch(e) { notify(e.message,true);record(`계정 연결 작업 오류: ${e.message}`); }
  finally { await syncAccount(); }
}
async function saveDemo(e) {
  e.preventDefault();
  if (state.environment !== 'demo') return;
  const form=e.currentTarget, button=form.querySelector('[type=submit]');button.disabled=true;
  const body={api_key:$('demoApiKey').value,secret_key:$('demoSecretKey').value,passphrase:$('savePassphrase').value};
  try {
    const result=await request('/api/v1/credentials/demo/save',{method:'POST',body:JSON.stringify(body)});
    updateCredentialList(result.credentials);notify('데모 키 암호화 저장 및 연결 완료');record('데모 API 연결 완료');
  } catch(err) {
    notify(err.message,true);record('데모 API 저장 또는 연결 실패');
    try { const bootstrap=await request('/api/v1/bootstrap');updateCredentialList(bootstrap.credentials); } catch { /* avoid hiding original error */ }
  } finally { form.reset();button.disabled=false;await syncAccount(); }
}
async function unlockDemo(e) {
  e.preventDefault();if(state.environment!=='demo')return;
  const form=e.currentTarget, button=form.querySelector('[type=submit]');button.disabled=true;
  try {await credentialPost('/api/v1/credentials/demo/unlock',{passphrase:$('unlockPassphrase').value},'데모 계정 연결 완료');}
  finally {form.reset();button.disabled=false;}
}
function renderModels() {
  const root=$('modelList'), stats=$('modelStatsBody');root.replaceChildren();
  $('selectedCount').textContent=`${state.selected.length}개`;
  for (const model of state.models) {
    const row=document.createElement('label');row.className='model-row';
    const cb=document.createElement('input');cb.type='checkbox';cb.value=model.id;cb.checked=state.selected.includes(model.id);cb.setAttribute('aria-label',`${model.name} 선택`);
    cb.addEventListener('change',()=>{
      if(cb.checked && root.querySelectorAll('input:checked').length>2){cb.checked=false;notify('읽기 전용 모델 계획은 최대 2개까지 선택할 수 있습니다.',true);}
      $('selectedCount').textContent=`${root.querySelectorAll('input:checked').length}개 (미저장)`;
      stats.querySelectorAll('tr').forEach(tr=>{if(tr.dataset.model===model.id && tr.querySelector('[data-chosen]'))tr.querySelector('[data-chosen]').textContent=cb.checked?'변경 예정':'미선택';});
    });
    const content=document.createElement('div');content.className='model-row-copy';
    const title=document.createElement('strong');title.textContent=model.name;
    const subtitle=document.createElement('small');subtitle.textContent=model.description;
    content.append(title,subtitle);const badge=document.createElement('em');badge.textContent=model.implemented?'등록됨 · 거래 잠금':'실행 미구현';
    row.append(cb,content,badge);root.append(row);

  }
  renderModelDashboard();
}
async function saveModels() {
  const ids=[...$('modelList').querySelectorAll('input:checked')].map(x=>x.value);
  const button=$('saveModelsBtn');button.disabled=true;
  try {
    const data=await request('/api/v1/models/selection',{method:'PUT',body:JSON.stringify({ids})});
    const runtime=await request('/api/v1/models/runtime');
    if(runtime.trading_authorized!==false || runtime.auto_start_locked!==true || runtime.execution_enabled!==false || JSON.stringify(runtime.selected_model_ids)!==JSON.stringify(data.selected_model_ids) || (ids.length<=1 && runtime.runtime_model_id!==(ids[0]||'')) || (ids.length>1 && (runtime.runtime_model_id!=='' || runtime.scheduler_mode!=='MULTI_READ_ONLY_PREVIEW' || typeof runtime.scheduler_running!=='boolean')))throw new Error('선택한 모델과 런타임 상태 검증 실패. 거래 기능은 잠금 상태입니다.');
    state.selected=data.selected_model_ids;state.modelDashboard=null;state.modelDashboardError="";renderModels();renderRuntimeHint(runtime);await Promise.all([syncModelSignals(),syncModelDashboard()]);
    notify(ids.length>1?'다중 모델 읽기 전용 선택 저장 완료 (주문 불가)':'모델 선택 저장 및 런타임 매핑 확인 완료 (Demo 주문 잠금 유지)');record(ids.length>1?'다중 모델 미리보기 구성: '+ids.join(', '):`실행 모델 선택 검증: ${runtime.runtime_model_id||'미선택'}`);
  }catch(e){notify(e.message,true);record('모델 전환 보류: '+e.message);}finally{button.disabled=false;}
}
function renderRuntimeHint(runtime){
 const el=$('modelRuntimeHint');if(!el)return;
 const active=runtime?.runtime_model_id;
 el.textContent=runtime?.scheduler_mode==='MULTI_READ_ONLY_PREVIEW'?'다중 모델 읽기 전용 선택: '+(runtime.scheduler_selected_ids||[]).join(', ')+(runtime.scheduler_running?' · 시세 관측 중':' · 시세 관측 대기')+' · 실제 주문 불가':active?`다음 실행 대상: ${active} · 실제 거래 잠금 유지`:'등록 모델을 선택하면 다음 실행 대상에 연결됩니다. 실제 거래는 잠금 상태입니다.';
}
async function syncModelSignals() {
 if(state.modelSignalBusy)return;
 state.modelSignalBusy=true;
 try {
  const info=await request('/api/v1/models/market-signals');
  if(info.trading_authorized!==false || info.execution_enabled!==false || info.ownership_verified!==false || info.risk_preview_ready!==false || info.assumption!=='HYPOTHETICAL_FLAT_NO_ACCOUNT_OR_OWNER_ATTESTATION')throw new Error('모델 관측의 주문 잠금/증거 구분이 유효하지 않습니다');
  if(JSON.stringify(info.selected_model_ids)!==JSON.stringify(state.selected))return;
  const list=$('modelSignalList');list.replaceChildren();
  const ready=info.state==='MARKET_SIGNALS_ONLY' && Array.isArray(info.signals) && info.signals.length===state.selected.length;
  if(!ready) {
   $('modelSignalStatus').textContent='관측 상태: '+String(info.state||'WAITING')+' · '+String(info.reason||'실시간 검증 대기');
   return;
  }
  $('modelSignalStatus').textContent='관측: '+kstTime(info.observed_at)+' · '+new Date(info.candle_open_ms).toLocaleTimeString('ko-KR',KST_FORMAT)+' 시작 1분봉 · signed 계정 '+(info.signed_account_fresh?'최근 수신':'미확인/오래됨')+' · 주문 불가';
  for(const signal of info.signals) {
    const row=document.createElement('div');row.className='model-signal-item';
    const name=document.createElement('span');name.className='model-signal-name';name.textContent=(state.models.find(m=>m.id===signal.model_id)?.name||signal.model_id)+' · '+String(signal.reason||'');
    const dir=document.createElement('b');dir.className='model-signal-direction '+(signal.direction==='LONG'?'long':signal.direction==='SHORT'?'short':'');
    dir.textContent=['LONG','SHORT','HOLD'].includes(signal.direction)?signal.direction:'BLOCKED';
    row.append(name,dir);list.append(row);
  }
 }catch(e){$('modelSignalStatus').textContent='모델 관측 실패 · '+e.message;$('modelSignalList').replaceChildren();}
 finally {state.modelSignalBusy=false;}
}
// One unified GET renders selected models, local positions and durable safety state.
// Never transform local quantity or historical gross estimates into model PnL.
function renderModelDashboard() {
  const view=state.modelDashboard;
  const sameSelection=view && JSON.stringify(view.selected_model_ids)===JSON.stringify(state.selected);
  const safe=!!sameSelection && state.environment==='demo';
  const root=$('modelStatsBody');root.replaceChildren();
  $('modelRiskState').textContent=safe?'거래 차단':'미확인 · 거래 차단';
  $('modelRiskState').classList.remove('good');
  if(!safe){
    $('autoRunSummary').textContent='● 상태 미확인 · 주문 잠금';
    $('multiModelEvidenceStatus').textContent=(state.modelDashboardError||'모델·계정 증거 조회 대기')+' · 주문 잠금 유지';
    for(const id of ['modelControlState','modelLifecycleState','modelSignedState','modelAccountingState'])$(id).textContent='미확인';
    $('modelBlockers').textContent='확인 가능한 안전 증거가 없습니다. START/STOP 주문 경로는 HTTP 423 잠금 상태입니다.';
    if(!state.models.length)tableHint('modelStatsBody',7,'등록된 모델이 없습니다.');
    else tableHint('modelStatsBody',7,'모델별 계정 증거 조회 대기 · 숫자를 0으로 추정하지 않습니다.');
    return;
  }
  const run=view.run_control||{}, controller=view.controller||{};
  const flags=controller.running||controller.fail_closed||controller.signed_review_required||run.restart_block;
  $('autoRunSummary').textContent=flags?'● 안전 조사 필요 · 주문 잠금':`● ${run.state||'상태 미확인'} · 주문 잠금`;
  $('modelControlState').textContent=`${run.state||'미확인'}${run.restart_block?' · 재시작 차단':''}${controller.signed_review_required?' · signed 재검증 필요':''}${controller.fail_closed?' · 장애 차단':''}`;
  $('modelLifecycleState').textContent=`차단 · 예약 ${view.active_reservations}건 / 미종료 ${view.lifecycle_pending}건`;
  $('modelSignedState').textContent=view.signed_account_fresh?'최근 signed 스냅샷 있음 · 전체 이력 미검증':'signed 계정 최근 스냅샷 없음';
  $('modelAccountingState').textContent=`과거 추정 보고서 ${view.accounting_report_count}건 · 확정 순손익 없음`;
  const signedQty=view.signed_account_fresh?`Binance LONG ${view.signed_long_qty} / SHORT ${view.signed_short_qty} BTC (계정 합산)`:'Binance 현재 수량 미확인';
  $('multiModelEvidenceStatus').textContent=`${view.local_snapshot_consistent?'현재 수량 대조 일치(과거 미검증)':'로컬/거래소 수량 대조 미확정'} · ${signedQty} · ${kstTime(view.observed_at)} 기준`;
  $('modelBlockers').textContent='차단 사유: '+(view.risk_blockers||[]).slice(0,3).join(' / ')+' · 모델별 포지션·손익 확정 아님';
  for(const m of view.models) {
    const info=state.models.find(x=>x.id===m.model_id);
    const name=info?.name||m.model_id;
    const local=(arr)=>Array.isArray(arr)&&arr.length?arr.map(x=>`${x} BTC`).join(' + ')+' (로컬, 미검증)':'기록 없음 ≠ 0 증명';
    const signal=['LONG','SHORT','HOLD'].includes(m.market_signal)?m.market_signal+' (시세 참고)':'시세 신호 대기';
    const tr=document.createElement('tr');tr.dataset.model=m.model_id;
    [name,m.selected?'선택됨':'미선택',signal+' · 주문 잠금',local(m.local_long_quantities),local(m.local_short_quantities),'— (미검증)','— (미검증)'].forEach((val,i)=>{
      const td=document.createElement('td');td.textContent=val;
      if(i===1)td.dataset.chosen='';
      if(i>=5)td.className='unverified-cell';
      tr.append(td);
    });
    root.append(tr);
  }
  if(!root.childElementCount)tableHint('modelStatsBody',7,'등록된 모델이 없습니다.');
}
async function syncModelDashboard(){
 if(state.modelEvidenceBusy || state.environment!=='demo')return;
 state.modelEvidenceBusy=true;
 const generation=state.generation, selected=JSON.stringify(state.selected);
 try {
  const view=await request('/api/v1/trading/model-dashboard');
  if(generation!==state.generation || selected!==JSON.stringify(state.selected))return;
  if(view.trading_authorized!==false || view.execution_enabled!==false || view.auto_start_locked!==true || view.auto_stop_locked!==true || view.manual_order_locked!==true || view.automatic_resume!==false || view.historical_trades_verified!==false || view.ownership_verified!==false || view.risk_preview_ready!==false || view.risk_state!=='BLOCKED_MANUAL_REVIEW_REQUIRED' || view.pnl_attribution_status!=='MODEL_FINAL_NET_AND_UNREALIZED_UNVERIFIED' || !Array.isArray(view.models) || !Array.isArray(view.risk_blockers) || !view.run_control || !view.controller || JSON.stringify(view.selected_model_ids)!==selected || view.models.some(m=>m.realized_pnl_status!=='UNVERIFIED'||m.unrealized_pnl_status!=='UNVERIFIED'))throw new Error('거래 잠금/정산 검증 계약 불일치');
  state.modelDashboard=view;state.modelDashboardError='';renderModelDashboard();
 }catch(err){
  if(generation===state.generation){state.modelDashboard=null;state.modelDashboardError='증거 확인 실패: '+err.message;renderModelDashboard();}
 }finally{state.modelEvidenceBusy=false;}
}
function tableHint(id, columns, text) {
  const root=$(id);root.replaceChildren();const tr=document.createElement('tr');const td=document.createElement('td');
  td.colSpan=columns;td.className='table-placeholder';td.textContent=text;tr.append(td);root.append(tr);
}
function addRow(root, cells, highlights={}) {
  const tr=document.createElement('tr');
  cells.forEach((value,index)=>{const td=document.createElement('td');td.textContent=String(value ?? '—');if(highlights[index])td.className=highlights[index];tr.append(td);});
  root.append(tr);
}
function renderAccount(snapshot) {
  state.account=snapshot;
  const connected=state.environment==='demo' && snapshot?.status==='connected';
  const status=state.environment==='real'?'실거래 잠금':connected?'데모 계정 연결됨':snapshot?.status==='error'?'데모 조회 오류':'데모 인증 대기';
  $('connectionText').textContent=status;$('footerConnection').textContent=status;
  const stateName=connected?'connected':snapshot?.status==='error'?'error':'locked';
  markDot('connectionDot',stateName);markDot('footerDot',stateName);
  $('accountFreshness').textContent=connected ? `계정 ${new Date(snapshot.as_of).toLocaleTimeString('ko-KR')}`:'실제 계정 데이터 없음';
  if(!connected){
    for(const id of ['availableBalance','walletBalance','unrealizedPnl','positionCount','positionTabCount','orderTabCount'])$(id).textContent='—';
    const hint=state.environment==='real'?'실거래 연결이 잠겨 있습니다':snapshot?.status==='error'?'데모 연결 오류 · 설정에서 다시 연결하세요':'데모 인증 후 실제 계정 데이터가 표시됩니다';
    tableHint('positionsBody',8,hint);tableHint('ordersBody',7,hint);tableHint('balancesBody',3,hint);
    updatePreview();renderWallet();return;
  }
  const positionsComplete=Array.isArray(snapshot.positions),ordersComplete=Array.isArray(snapshot.orders)&&Array.isArray(snapshot.algo_orders);
  const balances=Array.isArray(snapshot.balances)?snapshot.balances:[],positions=positionsComplete?snapshot.positions:[],orders=Array.isArray(snapshot.orders)?snapshot.orders:[],algos=Array.isArray(snapshot.algo_orders)?snapshot.algo_orders:[];
  const usdt=balances.find(b=>b.asset==='USDT');
  $('availableBalance').textContent=usdt?num(usdt.availableBalance,4):'—';
  $('walletBalance').textContent=usdt?num(usdt.balance,4):'—';
  // Missing/incomplete account position data must never render as 0 USDT.
  // Keep account-level totals independent of unverified model-level PnL.
  let pnl=positionsComplete?0:null;
  for(const position of positions){
    const value=parseAccountPnl(position?.unRealizedProfit);
    if(value===null || pnl===null || !Number.isFinite(pnl+value)){pnl=null;break;}
    pnl+=value;
  }
  $('unrealizedPnl').textContent=pnl===null?'— (미확인)':signed(pnl,4);
  $('unrealizedPnl').classList.toggle('positive',pnl!==null&&pnl>0);
  $('unrealizedPnl').classList.toggle('negative',pnl!==null&&pnl<0);
  $('positionCount').textContent=positionsComplete?String(positions.length):'—';$('positionTabCount').textContent=positionsComplete?String(positions.length):'—';$('orderTabCount').textContent=ordersComplete?String(orders.length+algos.length):'—';
  const pRoot=$('positionsBody');pRoot.replaceChildren();
  for (const p of positions) {
    const label=p.positionSide||'—';const val=parseAccountPnl(p.unRealizedProfit);
    addRow(pRoot,['거래소 합산',`${p.symbol} / ${label}`,p.positionAmt,num(p.entryPrice,5),num(p.markPrice,5),num(p.breakEvenPrice,5),val===null?'— (미확인)':`${signed(val,4)} USDT`,`${p.leverage||'—'}배 / ${p.marginType==='isolated'?'격리':p.marginType||'—'}`],{1:label==='LONG'?'long-side':label==='SHORT'?'short-side':'',6:val!==null&&val>0?'positive':val!==null&&val<0?'negative':''});
  }
  if (!positionsComplete) tableHint('positionsBody',8,'계정 포지션 목록이 누락되어 보유 여부를 확인할 수 없습니다.');
  else if (!positions.length) tableHint('positionsBody',8,'현재 BTCUSDT 보유 포지션이 없습니다.');
  const oRoot=$('ordersBody');oRoot.replaceChildren();
  for(const o of orders) addRow(oRoot,['일반',`${o.symbol} / ${o.positionSide} / ${o.side}`,o.type,`${o.executedQty||'0'} / ${o.origQty||'—'}`,Number(o.stopPrice)>0?num(o.stopPrice,5):num(o.price,5),'—',o.status]);
  for(const o of algos) addRow(oRoot,['조건부',`BTCUSDT / ${o.positionSide} / ${o.side}`,o.orderType,o.quantity||'—',num(o.triggerPrice,5),o.workingType||'—',o.algoStatus]);
  if (!ordersComplete)tableHint('ordersBody',7,'일반/조건부 주문 목록이 불완전하여 미체결 주문 여부를 확인할 수 없습니다.');
  else if (!orders.length&&!algos.length)tableHint('ordersBody',7,'현재 BTCUSDT 미체결 주문이 없습니다.');
  const bRoot=$('balancesBody');bRoot.replaceChildren();
  for(const b of balances) addRow(bRoot,[b.asset,num(b.balance,8),num(b.availableBalance,8)]);
  if(!balances.length)tableHint('balancesBody',3,'반환된 자산 항목이 없습니다.');
  updatePreview();renderWallet();
}
async function syncAccount() {
  if(state.accountBusy||!state.csrf)return;
  state.accountBusy=true;const generation=state.generation;
  try {
    const data=await request('/api/v1/account/snapshot');
    if(generation===state.generation)renderAccount(data);
  } catch(e) {
    if(generation===state.generation)renderAccount({status:'error',error:e.message});
  } finally {state.accountBusy=false;}
}
function selectTab(tab) {
  state.tab=tab;
  document.querySelectorAll('#bottomTabs button').forEach(btn=>{const active=btn.dataset.tab===tab;btn.classList.toggle('active',active);btn.setAttribute('aria-selected',String(active));});
  document.querySelectorAll('.tab-content').forEach(x=>x.classList.toggle('hidden',x.dataset.content!==tab));
  const notes={positions:'Binance 계정 합산 포지션 · 모델별 장부는 다음 단계에서 구현',orders:'일반 / 조건부 주문 모두 실제 Binance Demo 응답',models:'모델별 LONG/SHORT는 로컬 소유권 기록(검증되지 않음) · 신호는 공개 1분봉 참고 · 모델별 최종 손익/실제 소유권/주문 잠금',history:'불변 원본 기반 과거 회계 스냅샷 · 최종 손익 아님 · 실제 계정 스트림 미연결',balances:'실제 Binance Demo 계정에 반환된 자산별 수치',logs:'브라우저 화면 동작 기록 · Binance 거래 로그 아님'};
  $('bottomDescription').textContent=notes[tab];
  if(tab==='history')syncAccountingHistory();
}
function onManualInput() { state.manualPreviewGeneration++;$('manualPreviewResult').textContent='입력값이 변경됐습니다. 필요하면 서버 사전점검을 다시 실행하세요.';updatePreview(); }
function updatePreview() {
  const current=state.market?.candles?.at(-1)?.close;
  const margin=Number($('marginUSDT').value), lev=Number($('manualLeverage').value),tp=Number($('manualTP').value),sl=Number($('manualSL').value);
  const limit=Number($('limitPrice').value);
  const entry=state.orderType==='limit'?(limit>0?limit:null):current;
  const validPrice=Number.isFinite(entry)&&entry>0;
  const validLev=Number.isInteger(lev)&&lev>0&&lev<=125;
  const validROI=tp>0&&sl>0&&Number.isFinite(tp)&&Number.isFinite(sl)&&validLev;
  $('previewEntry').textContent=validPrice?`${num(entry,2)} USDT`:'—';
  $('previewNotional').textContent=Number.isFinite(margin)&&margin>0&&validLev?`${num(margin*lev,2)} USDT`:'—';
  if (!validPrice||!validROI) { $('previewLong').textContent='—';$('previewShort').textContent='—';return; }
  const t=tp/100/lev, s=sl/100/lev;
  $('previewLong').textContent=`${num(entry*(1+t),2)} / ${num(entry*(1-s),2)}`;
  $('previewShort').textContent=`${num(entry*(1-t),2)} / ${num(entry*(1+s),2)}`;
  // Informational gross ROI only; no fee/slippage assumptions.
}
// Server-side read-only preflight; the browser never supplies price, balances,
// exchange filters or signatures. Actual LONG/SHORT buttons stay disabled.
async function checkManualPreview(side) {
  const label=$('manualPreviewResult');
  if (state.environment !== 'demo' || state.account?.status !== 'connected') {
    label.textContent='Demo 계정을 먼저 연결해야 합니다.'; return;
  }
  const amount=$('marginUSDT').value.trim();
  const leverage=Number($('manualLeverage').value);
  if (!/^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?$/.test(amount) || Number(amount)<=0 || !Number.isInteger(leverage)) {
    label.textContent='유효한 투입 증거금과 정수 레버리지를 입력해 주세요.';return;
  }
  const limitPrice=$('limitPrice').value.trim();
  if(state.orderType==='limit' && (!/^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?$/.test(limitPrice) || Number(limitPrice)<=0)) {
    label.textContent='지정가 주문에는 유효한 지정 가격을 입력해 주세요.'; return;
  }
  const generation=state.generation, previewGeneration=++state.manualPreviewGeneration;
  label.textContent=`${side==='LONG'?'LONG':'SHORT'}: 최신 계정·시세·필터를 서버에서 조회하는 중…`;
  for (const id of ['previewManualLong','previewManualShort']) $(id).disabled=true;
  try {
    const result=await request('/api/v1/trading/manual/preview',{method:'POST',body:JSON.stringify({side,order_type:state.orderType==='limit'?'LIMIT':'MARKET',limit_price:state.orderType==='limit'?limitPrice:undefined,leverage,margin_usdt:amount})});
    if (generation!==state.generation || previewGeneration!==state.manualPreviewGeneration || state.environment!=='demo') return;
    if (result.can_place_order !== false || result.state!=='ESTIMATE_ONLY') throw new Error('예상치 못한 거래 권한 응답을 차단했습니다');
    label.textContent=`${side} ${state.orderType==='limit'?'지정가':'시장가'} 사전점검: 예상 수량 ${result.quantity_btc} BTC · 참고가 ${result.indicative_price_usdt} USDT · 참고 명목 ${result.indicative_notional_usdt} USDT. 실제 주문 불가.`;
    record(`${side} 서버 진입 참고 계산 완료 (실제 주문 없음)`);
  }catch(error){
    if (generation===state.generation && previewGeneration===state.manualPreviewGeneration) label.textContent=`서버 사전점검 불가: ${error.message}`;
  }finally{for(const id of ['previewManualLong','previewManualShort']) $(id).disabled=false;}
}
function marketFailure(text, wsStatus=null) {
  const failureKey=[wsStatus?.state||'',wsStatus?.message_count||0,wsStatus?.reconnect_count||0,wsStatus?.last_error||text||''].join('|');
  const needsRender=state.market!==null||state.indicators!==null||state.marketFailureKey!==failureKey;
  state.market=null;state.indicators=null;state.marketFailureKey=failureKey;
  const names={waiting:'웹소켓 연결 대기',connecting:'웹소켓 접속 중',syncing:'과거 봉과 동기화 중',
    reconnecting:'웹소켓 재연결 중',stale:'실시간 데이터 지연',locked:'실거래 화면 잠금'};
  const reason=wsStatus?.last_error||text||'시장 데이터 대기 중';
  $('marketStatus').textContent=names[wsStatus?.state]||'데모 실시간 시세 오류';
  $('marketAsOf').textContent=wsStatus ? `WS 메시지 ${wsStatus.message_count||0}개 · 재연결 ${wsStatus.reconnect_count||0}회` : '실제 WS 데이터 없음';
  markDot('marketDot','error');$('chartEmpty').classList.remove('hidden');
  $('chartEmptyTitle').textContent=names[wsStatus?.state]||'실시간 시세를 받지 못했습니다';
  $('chartEmptyDetail').textContent=reason+' · 신선한 WS 데이터가 없으므로 보조지표와 예상 가격을 숨깁니다.';
  if(needsRender)renderMarket();
}
function marketMessageChanged(previous,next){
  return !previous||Number(previous.message_count)!==Number(next.message_count);
}
async function syncMarket(manual=false) {
  if(state.marketBusy||state.environment!=='demo')return;
  state.marketBusy=true;const generation=state.generation;
  try {
    const result=await requestMarketLive();
    if(generation!==state.generation)return;
    if(!result.ready){
      marketFailure('실시간 시장 데이터가 아직 준비되지 않았습니다.',result.data);
      if(manual)notify('아직 정상적인 실시간 시세가 없습니다.',true);
      return;
    }
    const data=result.data;
    if(data.source!=='BINANCE_FUTURES_DEMO_WEBSOCKET'||data.symbol!=='BTCUSDT'||data.interval!=='1m'||
       data.market_venue!=='BINANCE_USDS_M_FUTURES_DEMO'||data.price_basis!=='CONTRACT_KLINE_LAST_PRICE'||
       data.event_time_basis!=='BINANCE_EVENT_TIME_UTC_MS'||data.freshness_basis!=='LOCAL_RECEIVE_AGE'||
       !data.ready||!Array.isArray(data.candles)||data.candles.length<100||!Number.isSafeInteger(Number(data.message_count)))throw new Error('WS 데이터 형식 오류');
    const messageChanged=marketMessageChanged(state.market,data);
    state.market=data;state.marketFailureKey='';
    $('marketStatus').textContent='Demo USDⓈ-M Futures · Contract Kline';
    const clockDelta=Number(data.event_receive_clock_delta_ms);
    const exchangeOffset=Number(data.exchange_clock_offset_ms);
    const clockNote=Number.isFinite(clockDelta)?` · 이벤트↔PC수신 ${clockDelta>=0?'+':''}${clockDelta}ms`:'';
    const offsetNote=Number.isFinite(exchangeOffset)?` · Binance↔PC ${exchangeOffset>=0?'+':''}${exchangeOffset}ms`:'';
    $('marketAsOf').textContent=`최근 수신 ${kstTime(data.last_received_at)} KST · 수신 경과 ${(data.age_ms/1000).toFixed(1)}초${clockNote}${offsetNote} · WS ${data.message_count}건`;
    markDot('marketDot','connected');$('chartEmpty').classList.add('hidden');
    if(messageChanged){state.indicators=calculateIndicators(data.candles);renderMarket();}
    if(manual)notify(messageChanged?'로컬 WebSocket 데이터 갱신 완료':'이미 최신 WebSocket 데이터입니다');
  }catch(e){if(generation===state.generation){marketFailure(e.message);if(manual)notify(e.message,true);}}
  finally {state.marketBusy=false;}
}
// All calculations use actual returned 1m closes (the current unfinished 1m
// candle is recalculated on every REST update). EMA seeds at its initial SMA,
// RSI is Wilder-smoothed, and MACD signal EMA starts at its 9th valid MACD.
function ema(input,period){
  const out=Array(input.length).fill(null);let sum=0,seen=0,last=null,k=2/(period+1);
  for(let i=0;i<input.length;i++){
    const v=input[i];if(v===null||!Number.isFinite(v))continue;
    if(last===null){sum+=v;seen++;if(seen===period){last=sum/period;out[i]=last;}}
    else {last=v*k+last*(1-k);out[i]=last;}
  }
  return out;
}
function sma(input,period){
  const out=Array(input.length).fill(null);let sum=0;
  for(let i=0;i<input.length;i++){sum+=input[i];if(i>=period)sum-=input[i-period];if(i>=period-1)out[i]=sum/period;}
  return out;
}
function wilderRSI(closes,period=14){
  const out=Array(closes.length).fill(null);if(closes.length<=period)return out;
  let gains=0,losses=0;
  const render=(gain,loss)=>gain===0&&loss===0?50:loss===0?100:100-100/(1+gain/loss);
  for(let i=1;i<=period;i++){const delta=closes[i]-closes[i-1];gains+=Math.max(delta,0);losses+=Math.max(-delta,0);}
  gains/=period;losses/=period;out[period]=render(gains,losses);
  for(let i=period+1;i<closes.length;i++){
    const diff=closes[i]-closes[i-1];gains=(gains*(period-1)+Math.max(diff,0))/period;losses=(losses*(period-1)+Math.max(-diff,0))/period;out[i]=render(gains,losses);
  }
  return out;
}
function calculateIndicators(candles){
  const close=candles.map(c=>c.close);
  const e7=ema(close,7),e25=ema(close,25),e99=ema(close,99),a25=sma(close,25),a99=sma(close,99),rsi=wilderRSI(close);
  const e12=ema(close,12),e26=ema(close,26);
  const macd=e12.map((x,i)=>x===null||e26[i]===null?null:x-e26[i]);
  const signal=ema(macd,9),hist=macd.map((v,i)=>v===null||signal[i]===null?null:v-signal[i]);
  return {ema7:e7,ema25:e25,ema99:e99,sma25:a25,sma99:a99,rsi,macd,signal,hist};
}
function canvasContext(id){
  const canvas=$(id),rect=canvas.getBoundingClientRect();
  if(rect.width<10||rect.height<10)return null;
  const dpr=Math.min(window.devicePixelRatio||1,2);
  canvas.width=Math.round(rect.width*dpr);canvas.height=Math.round(rect.height*dpr);
  const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);
  return {ctx,w:rect.width,h:rect.height};
}
const palette={ema7:'#e7b45d',ema25:'#d981bb',ema99:'#aa9be8',sma25:'#71afcb',sma99:'#8b9dac'};
function grid(ctx,w,h,lines,formatter){
  const right=68,left=10,plotW=w-right-left;
  ctx.strokeStyle='#2a3543';ctx.lineWidth=.6;ctx.font='10px Consolas,monospace';ctx.fillStyle='#778ba0';ctx.textAlign='left';
  lines.forEach(({y,label})=>{ctx.beginPath();ctx.moveTo(left,y);ctx.lineTo(w-right,y);ctx.stroke();ctx.fillText(formatter?formatter(label):label,w-right+7,y+3);});
  return {left,right,plotW};
}
function drawLine(ctx,values,indices,xAt,yAt,color,width=1.2){
  let begun=false;ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=width;
  for(let i=0;i<indices.length;i++){
    const v=values[indices[i]];
    if(v===null||!Number.isFinite(v)){begun=false;continue;}
    const x=xAt(i),y=yAt(v);
    if(!begun){ctx.moveTo(x,y);begun=true;}else ctx.lineTo(x,y);
  }
  ctx.stroke();
}
function drawPrice(candles,ind){
  const c=canvasContext('priceCanvas');if(!c)return;
  const {ctx,w,h}=c;const visible=Math.max(10,Math.min(90,Math.floor((w-90)/9)));
  const start=Math.max(0,candles.length-visible),indices=Array.from({length:candles.length-start},(_,j)=>start+j);
  const chosen=['ema7','ema25','ema99','sma25','sma99'].filter(k=>state.enabledOverlays.has(k));
  const vals=indices.flatMap(i=>[candles[i].low,candles[i].high,...chosen.map(k=>ind[k][i]).filter(x=>Number.isFinite(x))]);
  let lo=Math.min(...vals),hi=Math.max(...vals);const pad=Math.max((hi-lo)*.12,hi*.0001);lo-=pad;hi+=pad;
  const top=7,bottom=h-16,plotH=bottom-top;const y=v=>bottom-(v-lo)/(hi-lo)*plotH;
  const {left,plotW}=grid(ctx,w,h,Array.from({length:5},(_,i)=>({y:top+plotH*i/4,label:hi-(hi-lo)*i/4})),x=>num(x,1));
  const step=plotW/indices.length,bar=Math.max(2,Math.min(9,step*.62)),x=i=>left+step*(i+.5);
  ctx.lineWidth=1;
  indices.forEach((index,i)=>{const k=candles[index],color=k.close>=k.open?'#35bd91':'#ef637c';ctx.strokeStyle=color;ctx.fillStyle=color;
    ctx.beginPath();ctx.moveTo(x(i),y(k.high));ctx.lineTo(x(i),y(k.low));ctx.stroke();
    ctx.fillRect(x(i)-bar/2,Math.min(y(k.open),y(k.close)),bar,Math.max(1,Math.abs(y(k.close)-y(k.open))));
  });
  chosen.forEach(key=>drawLine(ctx,ind[key],indices,x,y,palette[key],key.startsWith('sma')?.9:1.25));
  const latest=candles.at(-1),priceY=y(latest.close);
  ctx.setLineDash([3,3]);ctx.strokeStyle='#8ba0ac';ctx.beginPath();ctx.moveTo(left,priceY);ctx.lineTo(w-68,priceY);ctx.stroke();ctx.setLineDash([]);
  ctx.fillStyle=latest.close>=latest.open?'#237e67':'#9c455a';ctx.fillRect(w-66,priceY-9,61,18);
  ctx.fillStyle='#e5f9f2';ctx.font='bold 10px Consolas,monospace';ctx.textAlign='left';ctx.fillText(num(latest.close,2),w-63,priceY+3);
  ctx.textAlign='center';ctx.fillStyle='#718499';ctx.font='9px Consolas,monospace';
  for(let i=0;i<indices.length;i+=Math.max(1,Math.ceil(indices.length/7))){
    ctx.fillText(new Date(candles[indices[i]].open_time).toLocaleTimeString('ko-KR',{hour:'2-digit',minute:'2-digit',hour12:false}),x(i),h-3);
  }
}
function drawRSI(indices,ind){
  const c=canvasContext('rsiCanvas');if(!c)return;const {ctx,w,h}=c,top=4,bottom=h-4,y=v=>bottom-v/100*(bottom-top);
  const {left,plotW}=grid(ctx,w,h,[70,50,30].map(v=>({y:y(v),label:v})),v=>String(v));
  const x=i=>left+plotW*(i+.5)/indices.length;
  drawLine(ctx,ind.rsi,indices,x,y,'#d17ec2',1.25);
}
function drawMACD(indices,ind){
  const c=canvasContext('macdCanvas');if(!c)return;const {ctx,w,h}=c;
  const vals=indices.flatMap(i=>[ind.macd[i],ind.signal[i],ind.hist[i]]).filter(Number.isFinite);
  const abs=Math.max(0.01,...vals.map(Math.abs))*1.12,top=5,bottom=h-5,y=v=>bottom-(v+abs)/(2*abs)*(bottom-top);
  const {left,plotW}=grid(ctx,w,h,[abs,0,-abs].map(v=>({y:y(v),label:v})),v=>num(v,1));
  const step=plotW/indices.length,x=i=>left+step*(i+.5),bar=Math.max(1,Math.min(5,step*.57));
  indices.forEach((index,i)=>{const v=ind.hist[index];if(!Number.isFinite(v))return;ctx.fillStyle=v>=0?'#2eae8d':'#de6579';ctx.fillRect(x(i)-bar/2,Math.min(y(v),y(0)),bar,Math.max(1,Math.abs(y(v)-y(0))));});
  drawLine(ctx,ind.macd,indices,x,y,'#bda3e5',1.1);drawLine(ctx,ind.signal,indices,x,y,'#e38cbb',1.1);
}
function renderMarket(){
  const candles=state.market?.candles,ind=state.indicators,ready=candles?.length>=100&&!!ind;
  const ids=['currentPrice','oneOpen','oneHigh','oneLow','oneVolume','ema7','ema25','ema99','sma25','sma99','rsi14','macdValue','signalValue','histogramValue','chartBarTime'];
  if(!ready){ids.forEach(id=>$(id).textContent='—');$('priceMove').textContent=state.environment==='real'?'실거래 시세 잠금':'데모 공개 시세 수신 대기';$('chartEmpty').classList.remove('hidden');
    ['priceCanvas','rsiCanvas','macdCanvas'].forEach(id=>{const c=canvasContext(id);if(c)c.ctx.clearRect(0,0,c.w,c.h);});updatePreview();return;}
  const k=candles.at(-1),previous=candles.at(-2),delta=k.close-previous.close;
  $('currentPrice').textContent=num(k.close,2);$('priceMove').textContent=`직전 1분 종가 대비 ${signed(delta,2)} (${signed(100*delta/previous.close,2)}%)`;
  $('priceMove').className=delta>0?'subtext up':delta<0?'subtext down':'subtext';
  for(const [id,value] of Object.entries({oneOpen:k.open,oneHigh:k.high,oneLow:k.low,oneVolume:k.volume}))$(id).textContent=num(value,id==='oneVolume'?3:2);
  for(const id of ['ema7','ema25','ema99','sma25','sma99','rsi14','macdValue','signalValue','histogramValue']){
    const key=({rsi14:'rsi',macdValue:'macd',signalValue:'signal',histogramValue:'hist'})[id]||id;
    const value=ind[key].at(-1);$(id).textContent=Number.isFinite(value)?num(value,key==='rsi'?2:key==='macd'||key==='hist'||key==='signal'?3:2):'—';
  }
  $('chartBarTime').textContent=`마지막 봉 ${kstTime(k.open_time)} KST · ${state.market?.current_bar_closed?'마감됨':'진행 중'} · Binance event time 기준`;
  const start=Math.max(0,candles.length-Math.max(10,Math.min(90,Math.floor(($('priceCanvas').getBoundingClientRect().width-90)/9))));
  const indices=Array.from({length:candles.length-start},(_,i)=>start+i);
  drawPrice(candles,ind);drawRSI(indices,ind);drawMACD(indices,ind);updatePreview();
}
function mouseTooltip(e){
  const data=state.market?.candles;if(!data?.length)return;
  const bounds=$('priceCanvas').getBoundingClientRect(),w=bounds.width;
  const visible=Math.max(10,Math.min(90,Math.floor((w-90)/9))),start=Math.max(0,data.length-visible),count=data.length-start;
  const index=Math.max(0,Math.min(count-1,Math.floor((e.clientX-bounds.left-10)/(w-78)*count)));
  const k=data[start+index];if(!k)return;
  $('chartTooltip').textContent=`${kstTime(k.open_time)} KST  시 ${num(k.open,2)}  고 ${num(k.high,2)}  저 ${num(k.low,2)}  종 ${num(k.close,2)}`;
}
function renderWallet() {
 const info=state.walletInfo,connected=state.environment==='demo'&&state.account?.status==='connected';
 $('walletTotalDisplay').textContent=connected?$('walletBalance').textContent+' USDT':'—';
 $('walletAssignedDisplay').textContent=info?.wallet?num(info.wallet.assigned_usdt,8)+' USDT':'—';
 $('autoWalletBalance').textContent=info?.wallet?num(info.wallet.assigned_usdt,4):'—';
 $('walletUnassignedDisplay').textContent=info && info.unallocated_usdt!=='' && info.unallocated_usdt!=null ?num(info.unallocated_usdt,8)+' USDT':'—';
 const allowed=connected&&!state.walletBusy;
 $('walletAllocate').disabled=!(allowed&&info?.can_allocate===true);
 $('walletReturn').disabled=!(allowed&&info?.can_return===true);
 $('walletNotice').textContent=!connected?'Demo 계정 연결이 필요합니다. 실제 자금 이체는 하지 않습니다.':
 !info?.wallet?'배정 정보를 확인하는 중…':
 !info.wallet.verified?'현재 API 키와 배정 기록이 다르거나 기록 확인에 실패했습니다. 수동 점검이 필요합니다.':
 !info.can_adjust?info.warning||'현재 보유 포지션·주문 또는 오래된 계정 정보로 배정 변경이 잠겨 있습니다.':
 `모든 변경에 저장된 Demo 암호와 전체 계정 서명 검증이 필요합니다. ${info.requires_reconciliation?'과거 기록 확인됨 · ':''}최신 거래소 가용액: ${num(info.account_free_usdt,8)} USDT · 기록 버전 ${info.wallet.revision}`;
}
async function syncWallet(){
 if(state.environment!=='demo'||!state.csrf)return;
 const generation=state.generation;
 try{const info=await request('/api/v1/wallet/auto');if(generation===state.generation){state.walletInfo=info;renderWallet();}}
 catch(e){if(generation===state.generation){state.walletInfo=null;renderWallet();record('배정 지갑 점검 필요: '+e.message);}}
}
async function changeWallet(op){
 if(state.walletBusy||state.environment!=='demo'||state.walletInfo?.[op==='ALLOCATE'?'can_allocate':'can_return']!==true)return notify('데모 계정과 배정 가능 상태를 확인하세요.',true);
 const amount=$('walletAmount').value.trim();
 if(!/^(?:0|[1-9][0-9]{0,12})(?:\.[0-9]{1,8})?$/.test(amount)||!(Number(amount)>0))return notify('유효한 양수 USDT 금액을 입력하세요.',true);
 const passphrase=$('walletPassphrase').value;
 if(!passphrase)return notify('모든 지갑 변경에 저장된 Demo 암호와 전체 계정 재검증이 필요합니다.',true);
 if(!confirm(`실제 거래소 송금이 아닌 내부 배정 변경입니다. ${amount} USDT를 ${op==='ALLOCATE'?'자동매매 예산에 추가':'내부 예산에서 차감'}할까요?`))return;
 state.walletBusy=true;renderWallet();
 try {
  await request('/api/v1/wallet/auto/change',{method:'POST',body:JSON.stringify({operation:op,amount_usdt:amount,request_id:crypto.randomUUID(),passphrase})});
  $('walletAmount').value='';notify('배정 기록이 로컬에 저장되었습니다. 실제 거래소 잔액은 이동하지 않았습니다.');await syncWallet();
 }catch(e){notify(e.message,true);await syncWallet();}finally{$('walletPassphrase').value='';state.walletBusy=false;renderWallet();}
}

// Report history is a separately built, read-only SQLite *projection* of
// already archived accounting reports. Never display it as live or verified
// actual net P&L and never convert money through Number before display.
async function syncAccountingHistory(){
 if(state.accountingBusy)return;
 state.accountingBusy=true;$('accountingRefresh').disabled=true;
 $('accountingIndexState').textContent='과거 색인 확인 중…';
 const owner=$('accountingOwner').value, modelID=$('accountingModel').value.trim();
 const filter=new URLSearchParams({limit:'20',offset:String(state.accountingOffset)});
 if(owner)filter.set('source',owner);
 if(owner==='model'&&modelID)filter.set('model_id',modelID);
 try{
   const page=await request('/api/v1/trading/accounting/history?'+filter.toString());
   const index=page.index||{};
   const summary=index.state==='NOT_BUILT'?'검증된 과거 회계 보고서 없음':`과거 보고서 ${index.report_count}개 · 읽기 색인 검증됨`;
   $('accountingIndexState').textContent=summary+' · 확정 손익 아님';
   const body=$('accountingHistoryBody');body.replaceChildren();
   for(const r of page.reports||[]){
     if(!r.owners?.length){addRow(body,[new Date(r.created_at).toLocaleString('ko-KR'),'해당 소유자 없음','—','—','—','—','—','—','—','—']);continue;}
     for(const o of r.owners){
       const fees=Object.entries(o.commission_by_asset||{}).map(([asset,amount])=>`${asset}: ${amount}`).join(' / ')||'미확인';
       const who=o.source==='model'?`모델 ${o.model_id}`:'수동';
       const protection=o.protection;
       const protectionText=protection?`TP ${protection.take_profit_quantity} / SL ${protection.stop_loss_quantity} · ${protection.fill_count}체결${protection.dual_triggered_group_count?` · 동시발동 ${protection.dual_triggered_group_count}그룹`:''}`:'없음';
       const funding=o.has_provisional_funding?`${o.provisional_funding_usdt} (잠정)`:'미배분/미확인';
       const slip=o.slippage_coverage==='MARKET_FILL_REFERENCE_ESTIMATE_ONLY'?`${o.signed_unfavorable_slippage_usdt} (참고)`:'미확인';
       const provisional=o.has_provisional_funding?o.provisional_after_funding_usdt+' (잠정)':'미확정';
       addRow(body,[new Date(r.created_at).toLocaleString('ko-KR'),who,o.position_side,o.open_quantity,
         o.virtual_gross_realized_usdt,fees,protectionText,funding,slip,provisional]);
     }
   }
   if(!body.childElementCount)tableHint('accountingHistoryBody',10,'해당 조건의 과거 회계 보고서가 없습니다. 현재 실시간 손익을 표시하지 않습니다.');
   state.accountingTotal=page.total||0;
   $('accountingPage').textContent=`보고서 ${state.accountingTotal}개 · ${state.accountingOffset+1}번째부터 (페이지당 20개)`;
   $('accountingPrevious').disabled=state.accountingOffset===0;
   $('accountingNext').disabled=state.accountingOffset+20>=state.accountingTotal;
 }catch(err){
   $('accountingIndexState').textContent='색인 미생성·오래됨 또는 검증 실패';
   tableHint('accountingHistoryBody',10,err.message+' · 원본 검사 후 report-index CLI 실행이 필요할 수 있습니다.');
   $('accountingPrevious').disabled=true;$('accountingNext').disabled=true;
 }finally{state.accountingBusy=false;$('accountingRefresh').disabled=false;}
}

async function boot(){
  clock();setInterval(clock,1000);
  $('settingsOpen').addEventListener('click',()=>{showSettings();inspectOperationsOverview();});
  $('settingsClose').addEventListener('click',()=>$('settingsDialog').close());
  $('demoBtn').addEventListener('click',()=>switchEnvironment('demo'));
  $('realBtn').addEventListener('click',()=>switchEnvironment('real'));
  $('demoSaveForm').addEventListener('submit',saveDemo);
  $('demoUnlockForm').addEventListener('submit',unlockDemo);
  $('lockDemoBtn').addEventListener('click',()=>credentialPost('/api/v1/credentials/demo/lock',{},'데모 계정 연결 종료'));
  $('refreshDemoBtn').addEventListener('click',()=>credentialPost('/api/v1/account/refresh',{},'계정 갱신 완료'));
  $('inspectConfigBtn').addEventListener('click',inspectDemoConfig);
  $('privateProbeForm').addEventListener('submit',inspectPrivateProbe);
  $('demoAccountingForm').addEventListener('submit',generateBoundedAccounting);
  $('demoRecoveryForm').addEventListener('submit',inspectRecoveryProbe);
  $('demoMonitorForm').addEventListener('submit',startContinuousMonitor);
  $('monitorStop').addEventListener('click',stopContinuousMonitor);
  $('monitorRefresh').addEventListener('click',inspectContinuousMonitor);
  $('recoveryHistory').addEventListener('click',inspectRecoveryHistory);
  $('operationsOverviewRefresh').addEventListener('click',inspectOperationsOverview);
  $('tradingReadinessRefresh').addEventListener('click',inspectTradingReadiness);
  $('operatorStatusRefresh').addEventListener('click',inspectOperatorStatus);
  $('operatorRemediationRefresh').addEventListener('click',inspectOperatorRemediation);
  $('operatorIncidentPacketsRefresh').addEventListener('click',inspectOperatorIncidentPackets);
  $('operatorIncidentTimelineRefresh').addEventListener('click',inspectOperatorIncidentTimeline);
  $('privateProbeHistory').addEventListener('click',inspectPrivateHistory);
  $('accountRefresh').addEventListener('click',()=>credentialPost('/api/v1/account/refresh',{},'계정 갱신 완료'));
  $('marketRefresh').addEventListener('click',()=>syncMarket(true));
  $('saveModelsBtn').addEventListener('click',saveModels);
  $('accountingRefresh').addEventListener('click',()=>{state.accountingOffset=0;syncAccountingHistory();});
  $('accountingOwner').addEventListener('change',()=>{
    $('accountingModel').disabled=$('accountingOwner').value!=='model';
    if($('accountingOwner').value!=='model')$('accountingModel').value='';
    state.accountingOffset=0;syncAccountingHistory();
  });
  $('accountingModel').disabled=true;
  $('accountingPrevious').addEventListener('click',()=>{state.accountingOffset=Math.max(0,state.accountingOffset-20);syncAccountingHistory();});
  $('accountingNext').addEventListener('click',()=>{if(state.accountingOffset+20<state.accountingTotal){state.accountingOffset+=20;syncAccountingHistory();}});
  $('previewManualLong').addEventListener('click',()=>checkManualPreview('LONG'));
  $('previewManualShort').addEventListener('click',()=>checkManualPreview('SHORT'));
  $('walletForm').addEventListener('submit',e=>{e.preventDefault();changeWallet('ALLOCATE');});
  $('walletReturn').addEventListener('click',()=>changeWallet('RETURN'));
  document.querySelectorAll('#bottomTabs button').forEach(btn=>btn.addEventListener('click',()=>selectTab(btn.dataset.tab)));
  document.querySelectorAll('.order-types button').forEach(btn=>btn.addEventListener('click',()=>{
    state.orderType=btn.dataset.orderType;document.querySelectorAll('.order-types button').forEach(x=>x.classList.toggle('active',x===btn));
    $('limitRow').classList.toggle('hidden',state.orderType!=='limit');onManualInput();
  }));
  for(const id of ['manualLeverage','limitPrice','marginUSDT','manualTP','manualSL'])$(id).addEventListener('input',onManualInput);
  document.querySelectorAll('[data-percent]').forEach(btn=>btn.addEventListener('click',()=>{
    if (state.environment!=='demo'||state.account?.status!=='connected')return notify('데모 계정 연결 후 사용할 수 있습니다.',true);
    const available=state.account.balances.find(b=>b.asset==='USDT')?.availableBalance;
    if(available===undefined)return notify('USDT 사용 가능 잔액이 없습니다.',true);
    $('marginUSDT').value=(Number(available)*Number(btn.dataset.percent)/100).toFixed(3);updatePreview();
  }));
  document.querySelectorAll('.legend-item').forEach(btn=>btn.addEventListener('click',()=>{
    const k=btn.dataset.indicator;
    if(state.enabledOverlays.has(k))state.enabledOverlays.delete(k);else state.enabledOverlays.add(k);
    btn.setAttribute('aria-pressed',String(state.enabledOverlays.has(k)));renderMarket();
  }));
  $('priceCanvas').addEventListener('mousemove',mouseTooltip);
  $('priceCanvas').addEventListener('mouseleave',()=>$('chartTooltip').textContent='캔들에 마우스를 올리면 시세를 확인할 수 있습니다');
  if(window.ResizeObserver){new ResizeObserver(()=>renderMarket()).observe($('priceCanvas').parentElement);}
  window.addEventListener('resize',renderMarket);
  try {
    const bootstrap=await request('/api/v1/bootstrap');
    if (bootstrap.trading_enabled) throw new Error('예상치 못한 서버 상태: 거래 기능이 잠겨 있지 않습니다. 업데이트를 중단합니다.');
    state.csrf=bootstrap.csrf_token;state.models=bootstrap.models||[];state.selected=bootstrap.selected_model_ids||[];
    applyEnvironment(bootstrap.environment);updateCredentialList(bootstrap.credentials);
    renderModels();renderAccount(bootstrap.account);selectTab('positions');
    const runtime=await request('/api/v1/models/runtime');
    if(runtime.trading_authorized!==false || runtime.auto_start_locked!==true)throw new Error('모델 런타임 안전 잠금 상태가 잘못되었습니다');
    renderRuntimeHint(runtime);
    record('로컬 서버 연결 완료 · 거래 기능 잠금');
    await Promise.all([syncMarket(),syncWallet(),inspectContinuousMonitor(),syncModelSignals(),syncModelDashboard()]);
    setInterval(inspectContinuousMonitor,12000);setInterval(syncModelSignals,1750);setInterval(syncModelDashboard,6000);setInterval(syncWallet,5000);setInterval(syncAccount,2000);setInterval(syncMarket,MARKET_POLL_INTERVAL_MS);
  }catch(e){notify(e.message,true);record(`초기 연결 오류: ${e.message}`);marketFailure('서버 연결 또는 초기화에 실패했습니다.');}
}
window.addEventListener('DOMContentLoaded',boot);
