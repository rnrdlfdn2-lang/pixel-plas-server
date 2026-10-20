// 그룹 서버: 그룹(초대코드·방장·멤버)을 기억하고, 접속한 멤버끼리 메시지를 전달한다.
// 그룹 정보는 groups.json, 최근 채팅은 messages.json 에 저장된다. 채팅은 RETAIN_HOURS(기본 24시간)가 지나면 자동으로 삭제된다.
const { WebSocketServer } = require('ws');
const fs = require('fs'), path = require('path'), crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'groups.json');
const MAX = 12, MAX_GROUPS = 5;

const groups = new Map(); // code -> {name, owner, members:[{uid,name,look}]}
try { for (const [c, g] of Object.entries(JSON.parse(fs.readFileSync(DATA, 'utf8')))) groups.set(c, g); } catch {}
let timer;
function persist() {
  clearTimeout(timer);
  timer = setTimeout(() => {
    try { fs.writeFileSync(DATA + '.tmp', JSON.stringify(Object.fromEntries(groups))); fs.renameSync(DATA + '.tmp', DATA); }
    catch (e) { console.error('저장 실패', e.message); }
  }, 300);
}

// ---- 최근 기록: 그룹별 채팅을 잠깐만 보관 (친구끼리 쓰는 용도라 짧게) ----
const MSG_FILE = process.env.MSG_FILE || path.join(__dirname, 'messages.json');
const RETAIN_MS = (Number(process.env.RETAIN_HOURS) || 24) * 3600 * 1000, MAX_MSGS = Number(process.env.MAX_MSGS) || 300;   // 기본: 24시간, 그룹당 300개
const msgLog = new Map(); // code -> [{id, ts, uid, name, text}] (오래된 것 -> 최신)
try { for (const [c, a] of Object.entries(JSON.parse(fs.readFileSync(MSG_FILE, 'utf8')))) msgLog.set(c, a); } catch {}
let mtimer;
function persistMsgs() {
  clearTimeout(mtimer);
  mtimer = setTimeout(() => {
    try { fs.writeFileSync(MSG_FILE + '.tmp', JSON.stringify(Object.fromEntries(msgLog))); fs.renameSync(MSG_FILE + '.tmp', MSG_FILE); }
    catch (e) { console.error('메시지 저장 실패', e.message); }
  }, 500);
}
function pruneMsgs() {
  const cut = Date.now() - RETAIN_MS;
  for (const [code, a] of msgLog) { const kept = a.filter((m) => m.ts >= cut); if (kept.length) msgLog.set(code, kept); else msgLog.delete(code); }
  persistMsgs();
}
function pushMsg(code, uid, name, text) {
  const a = msgLog.get(code) || [], ts = Math.max(Date.now(), (a.length ? a[a.length - 1].ts : 0) + 1);   // 시간이 겹치지 않게 항상 증가
  a.push({ id: ts.toString(36) + Math.random().toString(36).slice(2, 6), ts, uid, name, text });
  while (a.length > MAX_MSGS) a.shift();
  msgLog.set(code, a); persistMsgs();
}
setInterval(pruneMsgs, 3600 * 1000);

const rooms = new Map(); // code -> Map(connId -> ws)   (현재 접속 중인 연결)
let nextId = 1;
const send = (ws, m) => ws.readyState === 1 && ws.send(JSON.stringify(m));
function broadcast(code, m, exceptId) { for (const [id, c] of rooms.get(code) || []) if (id !== exceptId) send(c, m); }
function drop(code, pred, type, msg) { for (const w of [...(rooms.get(code) || []).values()]) if (pred(w)) { send(w, { t: type, msg, gone: true }); w.close(); } }   // gone: 이 그룹에서 완전히 빠짐

// ---- 보스 몬스터 ----
// 그룹마다 방장이 ON/OFF, 출현 주기(everyMin 분), 머무는 시간(lastsMin 분)을 정한다. 켜져 있으면 주기마다 확률로 보스 한 마리가 나타나 lastsMin 분 동안 머물고,
// 그룹원들이 보낸 공격(bhit)으로 체력 100 을 깎으면 처치 -> 공격한 사람들에게 경험치. 위치/움직임은 서버가 t0, seed 만 알려 주고 각 앱이 같은 식으로 계산한다.
const BOSS_TYPES = [['bat', 24, 120], ['liz', 22, 150], ['pig', 16, 200], ['mole', 22, 130], ['frog', 16, 140]];   // [종류, 나올 확률 가중치, 처치 경험치]
const BOSS_HP = 100, MAX_XP = 99999;
// ---- 차원 마왕: Lv6 이상이 3명 이상 접속해 있을 때만 나올 수 있는 특별 보스 ----
// 체력 600, 회피 20%, 체력이 400/200 이 되면 "심판": 한 명만 남기고 전원 기절 -> 그 한 명이 문제를 맞히면 계속, 틀리거나 시간 초과면 마왕이 도망감. 경험치 2배(400), 5번 잡으면 스태프 무기 해금
const LORD_HP = 600, LORD_EVADE = 0.2, LORD_POOL = 400, LORD_WEIGHT = 20, LORD_STEPS = [400, 200], LORD_ASK_MS = 30000, LORD_HEAL = 0.3, LORD_NEED_LV = 6, LORD_NEED_N = 1;
const LV_XP = [0, 50, 125, 225, 350, 500, 700, 950, 1250, 1600];
const lvOfXp = (xp) => { let lv = 1; while (lv < LV_XP.length && (xp || 0) >= LV_XP[lv]) lv++; return lv; };
const lordReady = (code) => { const g = groups.get(code); if (!g) return 0; const xp = g.xp || {}; return [...liveUids(code)].filter((u) => lvOfXp(xp[u]) >= LORD_NEED_LV).length; };
const bossRun = new Map();                                    // code -> {cur: 지금 보스, nextAt: 다음 출현 시각, dmg: Map(uid -> 준 피해), last: 마지막 일격 uid}   (저장하지 않는 실행 상태)
const runOf = (code) => { let r = bossRun.get(code); if (!r) { r = { cur: null, nextAt: 0, dmg: new Map(), last: null }; bossRun.set(code, r); } return r; };
const bossCfg = (g) => g.boss || (g.boss = { on: false, everyMin: 60, lastsMin: 10 });
const bossPublic = (b, now) => (b ? { id: b.id, type: b.type, seed: b.seed, t0: b.t0, until: b.until, hp: b.hp, max: b.max || BOSS_HP, judge: b.judge ? { wid: b.judge.wid, uid: b.judge.uid, until: b.judge.until } : null, now } : null);
function cfgFrom(m, old) {
  const on = typeof m.on === 'boolean' ? m.on : old.on, every = Math.round(Number(m.everyMin === undefined ? old.everyMin : m.everyMin)), lasts = Math.round(Number(m.lastsMin === undefined ? old.lastsMin : m.lastsMin));
  if (!(every >= 1 && every <= 1440)) fail('출현 주기는 1분에서 24시간 사이로 정해 주세요.');
  if (!(lasts >= 1 && lasts <= 180)) fail('머무는 시간은 1분에서 3시간 사이로 정해 주세요.');
  return { on, everyMin: every, lastsMin: lasts };
}
const online = (code) => (rooms.get(code) || new Map()).size;
function pickBoss(code, force) {
  const list = BOSS_TYPES.slice(); if (force === 'lord' || (code && lordReady(code) >= LORD_NEED_N)) list.push(['lord', LORD_WEIGHT, LORD_POOL]);
  if (force === 'lord') return list[list.length - 1];
  let r = Math.random() * list.reduce((a, b) => a + b[1], 0); for (const b of list) { if ((r -= b[1]) < 0) return b; } return list[0];
}
function spawnBoss(code, g, force) {
  const r = runOf(code), cfg = bossCfg(g), now = Date.now(), [type] = pickBoss(code, force), lord = type === 'lord';
  r.cur = { id: crypto.randomBytes(4).toString('hex'), type, seed: crypto.randomInt(1, 2147483647), t0: now, until: now + cfg.lastsMin * 60000, hp: lord ? LORD_HP : BOSS_HP, max: lord ? LORD_HP : BOSS_HP, th: 0, judge: null };
  r.dmg = new Map(); r.last = null; r.nextAt = now + cfg.everyMin * 60000;
  broadcast(code, { t: 'boss', boss: bossPublic(r.cur, now) });
}
function endBoss(code, g, reason) {
  const r = runOf(code), b = r.cur; if (!b) return; r.cur = null;
  const now = Date.now(), cfg = bossCfg(g); r.nextAt = Math.max(b.t0 + cfg.everyMin * 60000, now + 5000);
  if (reason !== 'dead') return broadcast(code, { t: 'bossend', id: b.id, reason });
  const pool = b.type === 'lord' ? LORD_POOL : (BOSS_TYPES.find((x) => x[0] === b.type) || BOSS_TYPES[0])[2], total = [...r.dmg.values()].reduce((a, c) => a + c, 0) || 1, rewards = [];
  g.xp = g.xp || {};
  for (const [uid, d] of r.dmg) {
    const mem = memberOf(g, uid); if (!mem) continue;
    const gain = Math.round(pool * (0.4 + 0.6 * (d / total)) + (uid === r.last ? pool * 0.15 : 0));       // 싸운 사람은 최소 40%, 준 피해 비율만큼 더, 마지막 일격은 +15%
    g.xp[uid] = Math.min(MAX_XP, (g.xp[uid] || 0) + gain); const rw = { uid, name: mem.name, dmg: d, gain, xp: g.xp[uid] };
    if (b.type === 'lord') { g.lordKills = g.lordKills || {}; rw.kills = g.lordKills[uid] = (g.lordKills[uid] || 0) + 1; }      // 마왕을 때려 본 사람은 처치 1회로 센다
    rewards.push(rw);
  }
  rewards.sort((a, c) => c.dmg - a.dmg); persist();
  broadcast(code, { t: 'bossend', id: b.id, reason: 'dead', type: b.type, last: r.last, rewards });
}
function startJudge(code, g) {                                   // 마왕 심판: 접속자 중 한 명이 답하는 사람, 나머지는 기절
  const b = runOf(code).cur, all = [...(rooms.get(code) || new Map()).values()], act = all.filter((w) => w.state !== 'away'), pool0 = act.length ? act : all, fresh = pool0.filter((w) => w.uid !== b.lastAns), ws = fresh.length ? fresh : pool0; if (!b || !ws.length) return;   // 문제마다 새로 뽑되, 가능하면 바로 전에 푼 사람은 빼고   // 자리 비운 사람보다 지금 보고 있는 사람을 먼저 뽑음
  const w = ws[crypto.randomInt(ws.length)], used = (b.used = b.used || new Set()), left = SUBJ_BANK.map((_, i) => i).filter((i) => !used.has(i)), qi = left[crypto.randomInt(left.length)], q = SUBJ_BANK[qi], now = Date.now(); used.add(qi);   // 한 판에서 같은 문제는 안 나옴
 
  b.judge = { uid: w.uid, wid: w.id, qi, until: now + LORD_ASK_MS, start: now }; b.lastAns = w.uid;
  broadcast(code, { t: 'bjudge', id: b.id, hp: b.hp, wid: w.id, uid: w.uid, name: (memberOf(g, w.uid) || {}).name || '', until: b.judge.until, now, q: { topicName: QUIZ_TOPICS[q.t], text: q.q, type: 'text' } });   // 문제는 모두에게 보임 (정답은 안 보냄)
}
function judgeFail(code, g, why, picked) {
  const b = runOf(code).cur; if (!b || !b.judge) return; const q = SUBJ_BANK[b.judge.qi];
  broadcast(code, { t: 'bjres', id: b.id, ok: false, why, answer: q.a[0], picked: picked === undefined ? null : String(picked) }); endBoss(code, g, 'flee');
}
setInterval(() => {                                           // 1초마다: 출현 시각이 됐는지 / 머무는 시간이 끝났는지
  const now = Date.now();
  for (const [code, g] of groups) {
    const cfg = g.boss, r = bossRun.get(code);
    if (r && r.cur && r.cur.judge && now > r.cur.judge.until + 500) judgeFail(code, g, 'timeout');
    else if (r && r.cur && !r.cur.judge && now >= r.cur.until) endBoss(code, g, 'time');
    if (!cfg || !cfg.on) continue; const rr = runOf(code);
    if (!rr.cur && now >= rr.nextAt) { if (online(code)) spawnBoss(code, g); else rr.nextAt = now + cfg.everyMin * 60000; }   // 아무도 접속 안 했으면 이번 차례는 건너뜀
  }
}, 1000);

// ---- 랜덤퀴즈 ----
// 방장이 ON/OFF 와 주기(분)를 정하면, 주기마다 "퀴즈 시작" 초대가 뜬다. 1분 안에 수락/거절 -> 전원이 답하거나 1분이 지나면 시작.
// 수락한 사람만 참여. 참여자 투표 비율로 주제 1개를 뽑아 그 주제에서 QUIZ_N 문제. 정답을 맞힌 사람은 모두 경험치. (정답은 서버만 알고, 문제가 나갈 때는 보기만 보냄)
const QUIZ_BANK = [{"t":"clock","q":"다음 중 일본에서 탄생한 시계 브랜드는?","c":["롤렉스","오메가","세이코","브라이틀링"],"a":2},{"t":"clock","q":"일반적인 쿼츠 시계가 시간을 측정하는 데 사용하는 것은?","c":["수정 진동자","태엽의 장력","진자의 왕복","원자 전이 주파수"],"a":0},{"t":"clock","q":"롤렉스의 대표적인 다이버 워치 컬렉션은?","c":["데이토나","익스플로러","데이트저스트","서브마리너"],"a":3},{"t":"clock","q":"오토매틱 기계식 시계가 손목의 움직임으로 태엽을 감는 데 주로 사용하는 부품은?","c":["크라운","로터","베젤","밸런스 휠"],"a":1},{"t":"clock","q":"크로노그래프의 대표적인 기능은?","c":["다른 시간대 표시","경과시간 측정","월별 날짜 보정","달의 위상 표시"],"a":1},{"t":"clock","q":"기계식 시계에서 파워리저브가 의미하는 것은?","c":["최대 방수 깊이","하루 허용 오차","완전히 감았을 때 작동 가능한 시간","밸런스의 진동수"],"a":2},{"t":"clock","q":"1969년 출시된 세이코 쿼츠 아스트론 35SQ가 유명한 이유는?","c":["최초의 상용 쿼츠 손목시계","최초의 오토매틱 손목시계","최초의 LCD 손목시계","최초의 GPS 손목시계"],"a":0},{"t":"clock","q":"GMT 기능의 주요 목적은?","c":["심박수 측정","잠수 시간 계산","달의 위상 확인","다른 시간대의 시각 표시"],"a":3},{"t":"clock","q":"다음 중 파텍 필립의 대표적인 컬렉션은?","c":["로열 오크","노틸러스","리베르소","산토스"],"a":1},{"t":"clock","q":"투르비용은 원래 어떤 문제를 줄이기 위해 고안되었는가?","c":["부식","자기장","중력에 따른 자세 오차","충격에 의한 유리 파손"],"a":2},{"t":"clock","q":"시계 사양에서 '28,800 vph'가 뜻하는 것은?","c":["시간당 진동 횟수","분당 회전수","하루 오차(초)","시간당 와인딩 횟수"],"a":0},{"t":"clock","q":"다음 중 시계의 컴플리케이션(기본 시각 표시 외의 부가 기능)에 해당하지 않는 것은?","c":["퍼페추얼 캘린더","미닛 리피터","사파이어 크리스털","문페이즈"],"a":2},{"t":"clock","q":"오메가 스피드마스터 프로페셔널과 가장 깊이 연관된 역사적 사건은?","c":["에베레스트 최초 등정","남극점 최초 도달","최초의 대서양 무착륙 횡단 비행","아폴로 11호 달 착륙"],"a":3},{"t":"clock","q":"다이버 워치의 단방향 회전 베젤이 주로 한 방향으로만 움직이도록 설계되는 안전상의 이유는?","c":["배터리 소모를 줄이기 위해","실수로 조작해도 경과시간이 실제보다 짧게 표시되는 것을 방지하기 위해","방수 성능을 높이기 위해","시침의 오차를 줄이기 위해"],"a":1},{"t":"clock","q":"스위스 크로노미터 인증기관 COSC가 주로 검증하는 것은?","c":["시계의 중고 거래 가치","무브먼트의 정확도","금속 소재의 순도","디자인의 독창성"],"a":1},{"t":"clock","q":"현대 시계의 일반적인 스위스 레버 이스케이프먼트에서 탈진기와 밸런스의 움직임을 연결하는 부품은?","c":["팔레트 포크","로터","날짜 디스크","배럴"],"a":0},{"t":"clock","q":"조지 다니엘스가 개발한 코액시얼 이스케이프먼트의 핵심 설계 목표는?","c":["날짜 표시의 자동화","탈진기 접촉면의 미끄럼 마찰 감소","태엽 없는 기계식 시계 구현","기계식 시계의 디지털 표시"],"a":1},{"t":"clock","q":"1972년 출시된 오데마 피게 로열 오크의 오리지널 디자인을 담당한 인물은?","c":["아브라함 루이 브레게","제랄드 젠타","한스 빌스도르프","조지 다니엘스"],"a":1},{"t":"clock","q":"28,800 vph인 기계식 무브먼트의 진동 주파수는 일반적인 Hz 기준으로 얼마인가? (1회 진동 = 반진동 2회)","c":["2Hz","3Hz","4Hz","5Hz"],"a":2},{"t":"clock","q":"퍼페추얼 캘린더가 일반적인 애뉴얼 캘린더와 다른 중요한 특징은? (그레고리력 기준의 일반적인 구현)","c":["주말을 자동 표시한다","윤년의 2월 날짜까지 기계적으로 반영한다","시간대 변경을 자동 인식한다","크로노그래프가 반드시 포함된다"],"a":1},{"t":"game","q":"《프래그마타(PRAGMATA)》를 개발한 게임 회사는?","c":["스퀘어 에닉스","캡콤","반다이 남코","코지마 프로덕션"],"a":1},{"t":"game","q":"《레드 데드 리뎀션 2》 본편의 주인공 이름은?","c":["존 마스턴","더치 반 더 린드","아서 모건","잭 마스턴"],"a":2},{"t":"game","q":"《더 라스트 오브 어스》 1편에서 조엘이 미국을 횡단하며 호송하는 소녀는?","c":["애비","엘리","디나","사라"],"a":1},{"t":"game","q":"《디아블로 IV》 본편에서 주요 적대자로 등장하는 악마는?","c":["안다리엘","디아블로","메피스토","릴리트"],"a":3},{"t":"game","q":"《갓 오브 워》(2018)에서 크레토스의 아들 이름은?","c":["발두르","아트레우스","티르","마그니"],"a":1},{"t":"game","q":"《프래그마타》에서 우주복을 입은 주인공 휴와 함께 행동하는 안드로이드 소녀의 이름은?","c":["에이다","다이애나","알로이","이브"],"a":1},{"t":"game","q":"《레드 데드 리뎀션 2》 본편의 주요 시대적 배경은?","c":["1865년","1880년","1899년","1911년"],"a":2},{"t":"game","q":"《더 라스트 오브 어스》 세계관에서 인류를 위협하는 감염 사태의 원인은?","c":["바이러스","기생 곰팡이","방사능","외계 미생물"],"a":1},{"t":"game","q":"《디아블로》 시리즈에서 천사와 악마의 전쟁이 벌어지는 가운데 인간들이 살아가는 세계의 이름은?","c":["아제로스","성역(생츄어리)","트리스트럼","판다리아"],"a":1},{"t":"game","q":"《엘든 링》에서 데미갓들이 차지해 서로 전쟁을 벌인 엘든 링의 파편은?","c":["위대한 룬","왕의 소울","고룡석","데미갓의 인장"],"a":0},{"t":"game","q":"《레드 데드 리뎀션 2》에서 아서 모건이 속한 갱단의 지도자는?","c":["마이카 벨","더치 반 더 린드","호제아 매튜스","콜름 오드리스콜"],"a":1},{"t":"game","q":"《바이오하자드 4》에서 레온이 구출하러 가는 인물은?","c":["질 발렌타인","클레어 레드필드","애슐리 그레이엄","레베카 체임버스"],"a":2},{"t":"game","q":"《사이버펑크 2077》에서 주인공 V의 머릿속에 디지털 인격으로 존재하는 인물은?","c":["아담 스매셔","재키 웰스","조니 실버핸드","타케무라 고로"],"a":2},{"t":"game","q":"《디아블로 II》에서 룬워드를 정상적으로 제작하기 위한 조건은?","c":["유니크 아이템에 룬을 장착한다","정해진 룬을 적합한 홈 아이템에 순서대로 장착한다","룬을 무작위로 조합한다","모든 홈에 같은 룬을 장착한다"],"a":1},{"t":"game","q":"《더 위쳐 3: 와일드 헌트》에서 게롤트가 찾아다니는, 특별한 혈통과 능력을 가진 인물은?","c":["트리스","예니퍼","시리","프리실라"],"a":2},{"t":"game","q":"《프래그마타》의 핵심 게임플레이 특징으로 가장 적절한 것은?","c":["턴제 카드 배틀","실시간 사격과 해킹 퍼즐의 결합","대규모 실시간 전략 전투","리듬 액션 기반 전투"],"a":1},{"t":"game","q":"《다크 소울 1》의 아노르 론도에서 등장하는 유명한 2인 보스 조합은?","c":["온슈타인과 스모우","시프와 아르토리우스","니토와 그윈","가고일과 쿠라그"],"a":0},{"t":"game","q":"《데스 스트랜딩》에서 샘 포터 브리지스가 주로 수행하는 활동은?","c":["현상금 사냥","물자 배송과 거점 연결","우주 기지 건설","군대 지휘"],"a":1},{"t":"game","q":"《레드 데드 리뎀션》 1편에서 존 마스턴이 추적하도록 강요받는 과거 갱단 동료 중 한 명은?","c":["레니 서머스","아서 모건","빌 윌리엄슨","션 맥과이어"],"a":2},{"t":"game","q":"《디아블로 IV》 세계관에서 릴리트와 함께 성역의 창조에 관여한 천사는?","c":["티리엘","이나리우스","임페리우스","말티엘"],"a":1},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 나츠키 스바루가 가진 특별한 능력은?","c":["미래 예지","사망귀환","시간 정지","기억 조작"],"a":1},{"t":"anime","q":"《전생했더니 슬라임이었던 건에 대하여》에서 리무루 템페스트가 전생한 종족은?","c":["드래곤","고블린","슬라임","마족"],"a":2},{"t":"anime","q":"《장송의 프리렌》에서 프리렌과 함께 마왕을 토벌한 용사의 이름은?","c":["힘멜","슈타르크","하이터","자인"],"a":0},{"t":"anime","q":"《오버로드》에서 아인즈 울 고운이 원래 플레이하던 가상현실 MMORPG의 이름은?","c":["아인크라드","위그드라실","엘더 테일","뉴 월드"],"a":1},{"t":"anime","q":"《무직전생》에서 주인공 루데우스 그레이랫이 어린 시절 마법을 배운 스승은?","c":["실피에트","에리스","록시","키시리카"],"a":2},{"t":"anime","q":"《소드 아트 온라인》의 첫 번째 게임에서 플레이어들이 갇힌 거대 부유성의 이름은?","c":["알브헤임","언더월드","아인크라드","건 게일"],"a":2},{"t":"anime","q":"《이 멋진 세계에 축복을!》에서 카즈마와 함께 이세계로 내려온 여신은?","c":["에리스","아쿠아","크리스","위즈"],"a":1},{"t":"anime","q":"《귀멸의 칼날》에서 최초로 등장하는 상현 3의 혈귀 이름은?","c":["도우마","코쿠시보","아카자","규타로"],"a":2},{"t":"anime","q":"《주술회전》에서 후시구로 메구미가 사용하는 대표적인 술식은?","c":["십종영법술","무하한 주술","적혈조술","투사주법"],"a":0},{"t":"anime","q":"《던전밥》에서 라이오스 일행이 던전을 탐험하며 마물을 먹는 가장 큰 이유는?","c":["전설의 요리책을 완성하기 위해","식량비를 아끼면서 파린을 구출하기 위해","던전의 왕이 되기 위해","마물 요리 대회에 참가하기 위해"],"a":1},{"t":"anime","q":"《메이드 인 어비스》에서 심계 6층에서 5층으로 상승할 때 일반적으로 나타나는 상승 부하의 대표적인 결과는?","c":["시력 저하","심한 두통","인간성 상실 또는 죽음","일시적인 기억 상실"],"a":2},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 렘과 람의 종족은?","c":["엘프","수인","오니","드워프"],"a":2},{"t":"anime","q":"《전생했더니 슬라임이었던 건에 대하여》에서 리무루가 처음 만나는 폭풍룡의 이름은?","c":["벨자드","벨그린드","베루도라(벨드라)","밀림"],"a":2},{"t":"anime","q":"《장송의 프리렌》에서 프리렌의 스승이었던 전설적인 마법사는?","c":["제리에","플람메","페른","위벨"],"a":1},{"t":"anime","q":"《노 게임 노 라이프》에서 소라와 시로가 소환된 세계의 이름은?","c":["디스보드","아인크라드","루그니카","나자릭"],"a":0},{"t":"anime","q":"《오버로드》에서 알베도가 소속된 나자릭 지하대분묘에서 맡고 있는 직책은?","c":["제1계층 수호자","수호자 총괄","전투 메이드 대장","정보부 총책임자"],"a":1},{"t":"anime","q":"《무직전생》에서 대규모 전이 재해가 발생한 사건을 통상 무엇이라 부르는가?","c":["피트아령 전이 사건","라플라스 전쟁","인마대전","아슬라 왕국 붕괴 사건"],"a":0},{"t":"anime","q":"《진격의 거인》에서 시조의 거인의 힘을 온전히 활용하는 데 중요한 특별한 혈통은?","c":["아커만 일족","동양인 일족","왕가의 혈통","전사 후보생 혈통"],"a":2},{"t":"anime","q":"《슈타인즈 게이트》에서 오카베 린타로가 자칭하는 매드 사이언티스트 이름은?","c":["존 타이터","호오인 쿄우마","미스터 브라운","아마데우스"],"a":1},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 탐욕의 마녀로 알려진 인물은?","c":["에키드나","사테라","미네르바","세크메트"],"a":0},{"t":"general","q":"지구에서 가장 큰 대륙은?","c":["아프리카","유럽","아시아","북아메리카"],"a":2},{"t":"general","q":"태양계에서 가장 큰 행성은?","c":["토성","목성","지구","해왕성"],"a":1},{"t":"general","q":"한글을 창제한 조선의 왕은?","c":["세종대왕","태종","정조","성종"],"a":0},{"t":"general","q":"우리 몸에서 가장 큰 장기는?","c":["간","폐","심장","피부"],"a":3},{"t":"general","q":"물의 화학식은?","c":["CO2","NaCl","H2O","O2"],"a":2},{"t":"general","q":"고대 이집트 문명이 발달한 강은?","c":["갠지스강","나일강","도나우강","티그리스강"],"a":1},{"t":"general","q":"윤년에 2월은 며칠까지 있을까?","c":["29일","31일","30일","28일"],"a":0},{"t":"general","q":"올림픽을 상징하는 오륜기의 고리는 몇 개?","c":["6개","7개","4개","5개"],"a":3},{"t":"general","q":"일본의 수도는?","c":["오사카","도쿄","교토","나고야"],"a":1},{"t":"general","q":"빛의 삼원색에 해당하지 않는 색은?","c":["초록","파랑","빨강","노랑"],"a":3},{"t":"general","q":"피카소가 대표적으로 이끈 미술 사조는?","c":["초현실주의","인상주의","입체주의(큐비즘)","팝아트"],"a":2},{"t":"general","q":"'모나리자'를 그린 화가는?","c":["레오나르도 다 빈치","미켈란젤로","라파엘로","반 고흐"],"a":0},{"t":"general","q":"소리가 전달되지 않는 곳은?","c":["철 막대 속","진공","물속","공기 중"],"a":1},{"t":"general","q":"대한민국 국보 1호는?","c":["훈민정음","불국사","숭례문","첨성대"],"a":2},{"t":"general","q":"광복절은 몇 월 며칠일까?","c":["6월 6일","10월 3일","3월 1일","8월 15일"],"a":3},{"t":"general","q":"사람의 정상 체온은 약 몇 도일까?","c":["약 36.5도","약 40도","약 34도","약 38도"],"a":0},{"t":"general","q":"세계에서 가장 높은 산은?","c":["몽블랑","에베레스트산","K2","킬리만자로"],"a":1},{"t":"general","q":"영국의 수도는?","c":["더블린","파리","런던","맨체스터"],"a":2},{"t":"general","q":"지구에서 가장 가까운 항성은?","c":["프록시마 센타우리","북극성","시리우스","태양"],"a":3},{"t":"general","q":"노벨상 분야가 아닌 것은?","c":["수학","물리학","화학","평화"],"a":0},{"t":"general","q":"영어 알파벳은 모두 몇 글자일까?","c":["26자","30자","28자","24자"],"a":0},{"t":"general","q":"혈액을 온몸으로 내보내는 장기는?","c":["신장","폐","심장","위"],"a":2},{"t":"general","q":"다음 중 포유류가 아닌 동물은?","c":["돌고래","박쥐","고래","펭귄"],"a":3},{"t":"general","q":"5만 원권 지폐에 그려진 인물은?","c":["이황","신사임당","이이","세종대왕"],"a":1},{"t":"general","q":"달이 지구를 한 바퀴 도는 데 걸리는 시간은 약 얼마일까?","c":["약 1년","약 일주일","약 하루","약 한 달"],"a":3},{"t":"general","q":"세계에서 면적이 가장 넓은 나라는?","c":["캐나다","중국","러시아","미국"],"a":2},{"t":"general","q":"소금의 주성분은?","c":["염화나트륨","염산","탄산칼슘","포도당"],"a":0},{"t":"general","q":"번개와 천둥 중 먼저 느껴지는 것은?","c":["항상 동시에","번개(빛이 더 빠르다)","천둥","매번 다르다"],"a":1},{"t":"general","q":"대한민국에서 가장 큰 섬은?","c":["강화도","거제도","제주도","울릉도"],"a":2},{"t":"general","q":"컴퓨터 'CPU'에서 C는 무엇의 약자일까?","c":["Computer","Control","Core","Central"],"a":3},{"t":"kpop","q":"'Dynamite'로 빌보드 핫 100 1위에 오른 그룹은?","c":["방탄소년단(BTS)","블랙핑크","트와이스","엑소"],"a":0},{"t":"kpop","q":"다음 중 블랙핑크 멤버가 아닌 사람은?","c":["로제","제니","리사","해린"],"a":3},{"t":"kpop","q":"뉴진스의 첫 EP 'New Jeans'에 수록된 곡이 아닌 것은?","c":["Attention","Hype Boy","Supernatural","Cookie"],"a":2},{"t":"kpop","q":"'강남스타일'을 부른 가수는?","c":["지드래곤","싸이","비","박진영"],"a":1},{"t":"kpop","q":"다음 중 방탄소년단 멤버가 아닌 사람은?","c":["지민","정국","RM","세훈"],"a":3},{"t":"kpop","q":"다음 중 소녀시대의 노래가 아닌 것은?","c":["Gee","Fantastic Baby","Genie","Into the New World"],"a":1},{"t":"kpop","q":"트와이스의 'TT' 안무에서 유명한 포즈는?","c":["브이 포즈","어깨춤","눈물 모양을 닮은 'TT' 손 모양","하트 손가락"],"a":2},{"t":"kpop","q":"BTS가 데뷔한 소속사(현 하이브 레이블)는?","c":["빅히트 뮤직","JYP엔터테인먼트","YG엔터테인먼트","SM엔터테인먼트"],"a":0},{"t":"kpop","q":"아이유 '좋은 날'에서 가장 유명한 파트는?","c":["아카펠라 파트","랩 파트","휘파람 파트","3단 고음"],"a":3},{"t":"kpop","q":"다음 중 에스파 멤버가 아닌 사람은?","c":["윈터","카리나","예지","닝닝"],"a":2},{"t":"kpop","q":"다음 중 빅뱅 멤버가 아닌 사람은?","c":["지드래곤","바비","대성","태양"],"a":1},{"t":"kpop","q":"'Love Dive'를 부른 그룹은?","c":["아이브","르세라핌","스테이씨","있지"],"a":0},{"t":"kpop","q":"'Hype Boy'를 부른 그룹은?","c":["뉴진스","아이브","르세라핌","에스파"],"a":0},{"t":"kpop","q":"엑소가 데뷔할 때 나뉘어 활동한 두 팀의 이름은?","c":["EXO-S, EXO-N","EXO-K, EXO-M","EXO-1, EXO-2","EXO-A, EXO-B"],"a":1},{"t":"kpop","q":"'Dynamite'는 어떤 언어로 발표된 곡일까?","c":["한국어","스페인어","일본어","영어"],"a":3},{"t":"kpop","q":"원더걸스의 대표곡은?","c":["Sorry Sorry","Gee","Tell Me","Mr. Taxi"],"a":2},{"t":"kpop","q":"슈퍼주니어의 'Sorry, Sorry'가 발표된 해는?","c":["2009년","2005년","2012년","2015년"],"a":0},{"t":"kpop","q":"'God's Menu'로 활동한 그룹은?","c":["세븐틴","투모로우바이투게더","스트레이 키즈","NCT 127"],"a":2},{"t":"kpop","q":"세븐틴은 몇 명으로 구성된 그룹일까?","c":["9명","13명","7명","11명"],"a":1},{"t":"kpop","q":"트와이스는 몇 명으로 구성된 그룹일까?","c":["5명","7명","12명","9명"],"a":3},{"t":"kpop","q":"블랙핑크의 소속사는?","c":["JYP엔터테인먼트","SM엔터테인먼트","빅히트 뮤직","YG엔터테인먼트"],"a":3},{"t":"kpop","q":"소녀시대가 데뷔한 해는?","c":["2014년","2007년","2011년","2003년"],"a":1},{"t":"kpop","q":"'보라해'라는 말을 만든 사람으로 알려진 BTS 멤버는?","c":["지민","정국","뷔","RM"],"a":2},{"t":"kpop","q":"BTS의 팬덤 이름은?","c":["아미(ARMY)","블링크(BLINK)","원스(ONCE)","엑소엘(EXO-L)"],"a":0},{"t":"kpop","q":"솔로곡 'MONEY'로 활동한 블랙핑크 멤버는?","c":["로제","지수","리사","제니"],"a":2},{"t":"kpop","q":"싸이의 '강남스타일' 뮤직비디오가 유튜브에서 처음 달성해 화제가 된 기록은?","c":["좋아요 1억 개","동시 접속자 100만 명","구독자 1억 명","조회수 10억 회"],"a":3},{"t":"kpop","q":"'Next Level'을 부른 그룹은?","c":["에스파","아이브","뉴진스","블랙핑크"],"a":0},{"t":"kpop","q":"엑소의 팬덤 이름은?","c":["블링크(BLINK)","엑소엘(EXO-L)","버니즈(Bunnies)","원스(ONCE)"],"a":1},{"t":"kpop","q":"'Love Shot'을 부른 그룹은?","c":["엑소","레드벨벳","NCT","샤이니"],"a":0},{"t":"kpop","q":"'Ditto'를 부른 그룹은?","c":["르세라핌","뉴진스","아이브","에스파"],"a":1},{"t":"movie","q":"영화 '기생충'이 아카데미 작품상을 받은 시상식이 열린 해는?","c":["2018년","2021년","2020년","2019년"],"a":2},{"t":"movie","q":"영화 '기생충'을 만든 감독은?","c":["봉준호","이창동","박찬욱","김지운"],"a":0},{"t":"movie","q":"드라마 '오징어 게임' 시즌1의 첫 번째 게임은?","c":["줄다리기","무궁화 꽃이 피었습니다","구슬치기","달고나 뽑기"],"a":1},{"t":"movie","q":"영화 '타이타닉'에서 잭 역을 맡은 배우는?","c":["브래드 피트","톰 크루즈","조니 뎁","레오나르도 디카프리오"],"a":3},{"t":"movie","q":"'어벤져스: 엔드게임'에서 타노스 역을 맡은 배우는?","c":["마크 러팔로","로버트 다우니 주니어","조슈 브롤린","크리스 에반스"],"a":2},{"t":"movie","q":"영화 '해리 포터'의 주인공이 다니는 마법 학교는?","c":["듀름스트랭","보바통","일버모니","호그와트"],"a":3},{"t":"movie","q":"영화 '겨울왕국'에서 엘사의 여동생 이름은?","c":["안나","올라프","크리스토프","한스"],"a":0},{"t":"movie","q":"영화 '반지의 제왕'에서 절대반지를 파괴하러 떠나는 호빗은?","c":["샘","프로도","피핀","빌보"],"a":1},{"t":"movie","q":"영화 '라이온 킹'의 주인공 사자 이름은?","c":["스카","나라","심바","무파사"],"a":2},{"t":"movie","q":"영화 '부산행'을 만든 감독은?","c":["류승완","연상호","나홍진","강제규"],"a":1},{"t":"movie","q":"드라마 '도깨비'에서 도깨비 역을 맡은 배우는?","c":["김고은","유인나","이동욱","공유"],"a":3},{"t":"movie","q":"드라마 '사랑의 불시착'의 두 주연 배우는?","c":["현빈, 손예진","박서준, 김다미","이민호, 전지현","송중기, 송혜교"],"a":0},{"t":"movie","q":"영화 '인셉션'을 만든 감독은?","c":["크리스토퍼 놀란","제임스 카메론","스티븐 스필버그","리들리 스콧"],"a":0},{"t":"movie","q":"영화 '아바타'(2009)를 만든 감독은?","c":["조지 루카스","마이클 베이","제임스 카메론","피터 잭슨"],"a":2},{"t":"movie","q":"영화 '친절한 금자씨'에서 금자 역을 맡은 배우는?","c":["전도연","이영애","김혜수","손예진"],"a":1},{"t":"movie","q":"드라마 '응답하라 1988'의 배경이 된 동네 이름은?","c":["상도동","신림동","이태원","쌍문동"],"a":3},{"t":"movie","q":"드라마 '더 글로리'에서 문동은 역을 맡은 배우는?","c":["김혜수","전지현","송혜교","한소희"],"a":2},{"t":"movie","q":"영화 '매트릭스'에서 네오가 고르는 두 가지 알약 색은?","c":["빨간 약과 파란 약","흰 약과 검은 약","보라 약과 주황 약","노란 약과 초록 약"],"a":0},{"t":"movie","q":"영화 '토이 스토리'에서 카우보이 인형의 이름은?","c":["햄","버즈","렉스","우디"],"a":3},{"t":"movie","q":"영화 '극한직업'에서 주인공들이 위장 창업한 가게는?","c":["분식집","치킨집","카페","국밥집"],"a":1},{"t":"movie","q":"영화 '명량'에서 이순신 역을 맡은 배우는?","c":["송강호","황정민","설경구","최민식"],"a":3},{"t":"movie","q":"영화 '택시운전사'에서 주인공 택시운전사 역을 맡은 배우는?","c":["송강호","최민식","유해진","하정우"],"a":0},{"t":"movie","q":"'MCU'에서 아이언맨(토니 스타크) 역을 맡은 배우는?","c":["제레미 레너","크리스 에반스","로버트 다우니 주니어","크리스 헴스워스"],"a":2},{"t":"movie","q":"드라마 '왕좌의 게임'의 원작 소설 시리즈 제목은?","c":["드래곤 연대기","얼음과 불의 노래","철의 왕좌","반지 전쟁"],"a":1},{"t":"movie","q":"영화 '쥬라기 공원'에서 공룡을 되살리는 데 쓴 핵심 재료는?","c":["호박 속 모기의 DNA","화석 뼈 직접 복원","냉동 보존된 공룡","외계 기술"],"a":0},{"t":"movie","q":"영화 '라라랜드'의 두 주연 배우는?","c":["티모시 샬라메, 젠데이아","라이언 고슬링, 엠마 스톤","마일즈 텔러, 엠마 왓슨","톰 홀랜드, 플로렌스 퓨"],"a":1},{"t":"movie","q":"드라마 '태양의 후예'에서 유시진 대위 역을 맡은 배우는?","c":["이종석","조인성","송중기","박보검"],"a":2},{"t":"movie","q":"영화 '헤어질 결심'으로 칸영화제 감독상을 받은 감독은?","c":["홍상수","이창동","봉준호","박찬욱"],"a":3},{"t":"movie","q":"영화 '스파이더맨' 주인공의 본명은?","c":["마일스 모랄레스","스티브 로저스","브루스 웨인","피터 파커"],"a":3},{"t":"movie","q":"영화 '다크 나이트'에서 조커 역으로 사후 아카데미 남우조연상을 받은 배우는?","c":["잭 니콜슨","히스 레저","호아킨 피닉스","자레드 레토"],"a":1},{"t":"history","q":"임진왜란이 일어난 해는?","c":["1392년","1636년","1592년","1910년"],"a":2},{"t":"history","q":"훈민정음이 반포된 해는?","c":["1446년","1592년","1894년","1392년"],"a":0},{"t":"history","q":"고려를 세운 인물은?","c":["견훤","왕건","이성계","궁예"],"a":1},{"t":"history","q":"조선을 세운 인물은?","c":["이방원","정도전","왕건","이성계"],"a":3},{"t":"history","q":"거북선을 만든 것으로 알려진 장군은?","c":["곽재우","권율","김시민","이순신"],"a":3},{"t":"history","q":"3·1 운동이 일어난 해는?","c":["1919년","1910년","1950년","1945년"],"a":0},{"t":"history","q":"6·25 전쟁이 시작된 해는?","c":["1945년","1950년","1953년","1960년"],"a":1},{"t":"history","q":"대한민국 정부가 수립된 해는?","c":["1950년","1945년","1948년","1953년"],"a":2},{"t":"history","q":"광개토대왕의 업적을 기록한 비석은?","c":["충주고구려비","진흥왕순수비","광개토대왕릉비","무령왕릉비"],"a":2},{"t":"history","q":"제1차 세계대전이 시작된 해는?","c":["1939년","1914년","1905년","1918년"],"a":1},{"t":"history","q":"제2차 세계대전이 끝난 해는?","c":["1945년","1939년","1950년","1918년"],"a":0},{"t":"history","q":"프랑스 혁명이 일어난 해는?","c":["1917년","1815년","1689년","1789년"],"a":3},{"t":"history","q":"콜럼버스가 아메리카 대륙에 도착한 해는?","c":["1492년","1692년","1592년","1392년"],"a":0},{"t":"history","q":"로마 제국의 초대 황제는?","c":["네로","아우구스투스","카이사르","콘스탄티누스"],"a":1},{"t":"history","q":"중국을 처음 통일하고 만리장성을 잇게 한 황제는?","c":["강희제","당 태종","진시황","한 무제"],"a":2},{"t":"history","q":"삼국을 통일한 나라는?","c":["백제","고구려","발해","신라"],"a":3},{"t":"history","q":"세종대왕 때 만든 해시계의 이름은?","c":["앙부일구","혼천의","자격루","측우기"],"a":0},{"t":"history","q":"조선 시대 최고 교육기관은?","c":["서원","서당","향교","성균관"],"a":3},{"t":"history","q":"고려가 몽골의 침입에 맞서 임시 수도로 삼은 섬은?","c":["거제도","제주도","강화도","진도"],"a":2},{"t":"history","q":"한산도 대첩에서 이순신 장군이 사용한 유명한 진법은?","c":["방진","학익진","어린진","장사진"],"a":1},{"t":"history","q":"안중근 의사가 이토 히로부미를 저격한 곳은?","c":["하얼빈역","상하이역","도쿄역","서울역"],"a":0},{"t":"history","q":"대한민국 임시정부가 처음 세워진 도시는?","c":["하얼빈","상하이","도쿄","베이징"],"a":1},{"t":"history","q":"현존하는 세계에서 가장 오래된 금속활자 인쇄본은?","c":["조선왕조실록","훈민정음","팔만대장경","직지심체요절"],"a":3},{"t":"history","q":"이순신 장군이 전쟁 중에 쓴 일기의 이름은?","c":["열하일기","징비록","난중일기","백범일지"],"a":2},{"t":"history","q":"허준이 쓴 의학서는?","c":["향약구급방","동의보감","본초강목","동의수세보원"],"a":1},{"t":"history","q":"고조선을 세운 것으로 전해지는 인물은?","c":["온조","주몽","박혁거세","단군왕검"],"a":3},{"t":"history","q":"고구려를 세운 인물은?","c":["박혁거세","김수로","주몽(동명성왕)","온조"],"a":2},{"t":"history","q":"산업혁명이 처음 시작된 나라는?","c":["영국","독일","미국","프랑스"],"a":0},{"t":"history","q":"미국 독립선언서가 발표된 해는?","c":["1676년","1812년","1776년","1865년"],"a":2},{"t":"history","q":"베를린 장벽이 무너진 해는?","c":["1991년","1989년","1961년","1975년"],"a":1},{"t":"meme","q":"인터넷 용어 'ㅇㅈ'의 뜻은?","c":["이제","인증","인정","예쁘다"],"a":2},{"t":"meme","q":"인터넷 용어 'ㄹㅇ'의 뜻은?","c":["랜덤","리얼(진짜)","라이언","라인"],"a":1},{"t":"meme","q":"'갑분싸'의 뜻은?","c":["갑부가 분양을 싸게 함","갑돌이가 분이와 싸움","갑자기 분노가 싸움으로 번짐","갑자기 분위기가 싸해짐"],"a":3},{"t":"meme","q":"'TMI'는 무엇의 줄임말일까?","c":["Too Much Information","Time Machine Item","Top Music Idol","Today My Idea"],"a":0},{"t":"meme","q":"'OOTD'의 뜻은?","c":["오늘의 옷차림","오늘의 일기","오늘의 운동","오늘의 음식"],"a":0},{"t":"meme","q":"'얼죽아'의 뜻은?","c":["얼음이 죽 같아","얼굴이 죽도록 아름다움","얼어 죽어도 아이스 아메리카노","얼마나 죽었어"],"a":2},{"t":"meme","q":"'칼퇴'의 뜻은?","c":["칼을 퇴장시킨다","칼같이 퇴근한다","칼국수를 퇴근길에 먹는다","칼로 퇴치한다"],"a":1},{"t":"meme","q":"MBTI는 몇 가지 성격 유형으로 나눌까?","c":["8가지","12가지","24가지","16가지"],"a":3},{"t":"meme","q":"유튜브에서 구독자 10만 명을 달성하면 받는 상은?","c":["실버 버튼","다이아 버튼","브론즈 버튼","골드 버튼"],"a":0},{"t":"meme","q":"'이모티콘'은 감정(emotion)과 무엇이 합쳐진 말일까?","c":["인터넷(internet)","이미지(image)","아이콘(icon)","인터뷰(interview)"],"a":2},{"t":"meme","q":"SNS에서 'DM'의 뜻은?","c":["디지털 미디어","다이렉트 메시지","데일리 미션","도메인 관리"],"a":1},{"t":"meme","q":"'알고리즘이 나를 이끌었다'라는 말에서 알고리즘은 주로 무엇을 뜻할까?","c":["비밀번호","계산기","검색 금지어","추천 시스템"],"a":3},{"t":"meme","q":"게임에서 'GG'는 무슨 뜻일까?","c":["Go Ground","Get Gold","Great Gift","Good Game"],"a":3},{"t":"meme","q":"인터넷 밈 'Doge'의 주인공 동물은?","c":["허스키","시바 이누","푸들","불독"],"a":1},{"t":"meme","q":"'심쿵'의 뜻은?","c":["심야에 쿵 소리가 남","심하게 쿵쾅 소리가 남","심장이 쿵 하고 뛸 만큼 설렘","심심해서 쿵쿵거림"],"a":2},{"t":"meme","q":"'핫플'의 뜻은?","c":["인기 있는 장소(핫플레이스)","뜨거운 플라스틱","핫도그 플레이트","뜨거운 플레이어"],"a":0},{"t":"meme","q":"'먹방'의 뜻은?","c":["먹이 방출","먹는 방송","먹는 방법","먹구름 방향"],"a":1},{"t":"meme","q":"'인싸'의 뜻은?","c":["무리 안에 잘 어울리는 사람","인생 싸움","인기 싸구려","인터넷 싸움꾼"],"a":0},{"t":"meme","q":"'플렉스(Flex)'는 어떤 뜻으로 쓰일까?","c":["요리를 하다","운동을 쉬다","돈이나 물건을 과시하다","공부를 하다"],"a":2},{"t":"meme","q":"'밈(meme)'이라는 말을 처음 만든 학자는?","c":["스티븐 호킹","찰스 다윈","칼 세이건","리처드 도킨스"],"a":3},{"t":"meme","q":"'@' 기호를 한국에서 흔히 부르는 이름은?","c":["골뱅이","해파리","달팽이","올챙이"],"a":0},{"t":"meme","q":"채팅에서 'ㅋㅋ'는 무엇을 나타낼까?","c":["졸림","웃음","울음","화남"],"a":1},{"t":"meme","q":"'갓생'의 뜻은?","c":["아무것도 안 하는 삶","게임만 하는 삶","부지런하고 모범적으로 사는 삶","신에게 기도하는 삶"],"a":2},{"t":"meme","q":"'카공족'은 어떤 사람들일까?","c":["카메라 공모전 참가자","카드 공유 모임","카레를 공짜로 먹는 사람들","카페에서 공부하는 사람들"],"a":3},{"t":"meme","q":"'현타'의 뜻은?","c":["현실 자각 타임","현금 타이머","현장 타격","현관 타일"],"a":0},{"t":"meme","q":"QR코드에서 QR은 무엇의 약자일까?","c":["Quality Rate","Question Row","Quiet Reader","Quick Response"],"a":3},{"t":"meme","q":"'와이파이(Wi-Fi)'가 가리키는 것은?","c":["위성 TV","무선 인터넷 접속 기술","유선 전화","라디오 방송"],"a":1},{"t":"meme","q":"영상 플랫폼에서 '좋아요'를 누를 때 흔히 쓰는 아이콘은?","c":["별표","느낌표","엄지 척(👍)","하트 깨짐"],"a":2},{"t":"meme","q":"'ASMR'에 대한 설명으로 맞는 것은?","c":["소리 등으로 편안함을 느끼는 감각 반응","자동 음악 재생","아시아 음악 대회","앱 서비스 관리"],"a":0},{"t":"meme","q":"넷플릭스처럼 정해진 비용을 내고 이용하는 방식은?","c":["경매","구독","할부","대여점"],"a":1},{"t":"nonsense","q":"입은 있지만 말은 못 하고, 목은 있지만 머리는 없는 것은?","c":["가방","항아리","인형","병"],"a":3},{"t":"nonsense","q":"손은 있는데 손가락이 없고, 얼굴은 있는데 눈코입이 없는 것은?","c":["동전","시계","장갑","거울"],"a":1},{"t":"nonsense","q":"왕이 넘어지면?","c":["킹콩","퀸카","왕창","소리"],"a":0},{"t":"nonsense","q":"개가 사람을 가르칠 때 쓰는 말은?","c":["개그","개구리","개인지도","개조식"],"a":2},{"t":"nonsense","q":"세상에서 가장 뜨거운 과일은?","c":["체리","자몽","수박","천도복숭아"],"a":3},{"t":"nonsense","q":"오리가 얼면?","c":["언덕","오리고기","얼음땡","꽥꽥"],"a":0},{"t":"nonsense","q":"세상에서 가장 큰 코는?","c":["코뿔소","콧구멍","멕시코","코끼리"],"a":2},{"t":"nonsense","q":"세상에서 가장 비싼 물고기는?","c":["참치","금붕어","광어","고등어"],"a":1},{"t":"nonsense","q":"세상에서 가장 빠른 닭은?","c":["오골계","후다닭","닭강정","치킨"],"a":1},{"t":"nonsense","q":"항상 오고 있지만 절대 도착하지 않는 것은?","c":["오늘","지금","어제","내일"],"a":3},{"t":"nonsense","q":"깎을수록 커지는 것은?","c":["구멍","손톱","연필","사과"],"a":0},{"t":"nonsense","q":"먹을수록 늘어나는 것은?","c":["머리숱","돈","나이","키"],"a":2},{"t":"nonsense","q":"던지면 다시 돌아오는 물건은?","c":["화살","돌","부메랑","공"],"a":2},{"t":"nonsense","q":"오른손을 들고 거울을 보면, 거울 속 내 모습은 어느 손을 든 것처럼 보일까?","c":["왼손","오른손","발","양손"],"a":0},{"t":"nonsense","q":"사과 3개 중 2개를 가져가면, 내가 가진 사과는 몇 개일까?","c":["1개","3개","0개","2개"],"a":3},{"t":"nonsense","q":"달걀 한 판은 몇 개일까?","c":["20개","30개","12개","10개"],"a":1},{"t":"nonsense","q":"한 시간은 몇 초일까?","c":["3600초","7200초","1800초","600초"],"a":0},{"t":"nonsense","q":"1부터 10까지 모두 더하면?","c":["60","45","55","50"],"a":2},{"t":"nonsense","q":"2의 5제곱은?","c":["16","32","25","64"],"a":1},{"t":"nonsense","q":"삼각형의 세 내각의 합은?","c":["90도","360도","270도","180도"],"a":3},{"t":"nonsense","q":"하루는 몇 분일까?","c":["1200분","1440분","2400분","1000분"],"a":1},{"t":"nonsense","q":"다섯 손가락 중 가장 굵은 손가락은?","c":["엄지","약지","검지","중지"],"a":0},{"t":"nonsense","q":"아무리 먹어도 배부르지 않은 것은?","c":["떡","밥","욕","빵"],"a":2},{"t":"nonsense","q":"겉은 딱딱하고 속은 부드러운데, 깨뜨려야 먹을 수 있는 것은?","c":["바나나","사과","빵","계란"],"a":3},{"t":"nonsense","q":"내 것인데 남이 더 많이 쓰는 것은?","c":["연필","지갑","우산","이름"],"a":3},{"t":"nonsense","q":"빵은 빵인데 먹을 수 없는 빵은?","c":["식빵","건빵","프라이팬","단팥빵"],"a":2},{"t":"nonsense","q":"속담 '수박 겉핥기'의 뜻은?","c":["가장 맛있는 부분만 먹음","내용을 제대로 알지 못하고 대충 아는 척함","수박을 열심히 먹음","수박을 반으로 가름"],"a":1},{"t":"nonsense","q":"속담 '원숭이도 나무에서 떨어진다'의 뜻은?","c":["아무리 능숙한 사람도 실수할 때가 있다","원숭이는 나무를 못 탄다","원숭이는 겁이 많다","높은 곳은 위험하다"],"a":0},{"t":"nonsense","q":"속담 '고래 싸움에 새우 등 터진다'의 뜻은?","c":["바다는 위험하다","강한 자들의 싸움에 약한 자가 피해를 본다","새우는 싸움을 좋아한다","고래가 새우를 구해 준다"],"a":1},{"t":"nonsense","q":"속담 '소 잃고 외양간 고친다'의 뜻은?","c":["소를 팔아 돈을 번다","소를 잘 지킨다","외양간을 새로 짓는다","이미 일을 그르친 뒤에 뒤늦게 대비한다"],"a":3}];
const QUIZ_TOPIC_LIST = [['clock', '⌚', '시계'], ['game', '🎮', '게임'], ['anime', '🍥', '애니'], ['general', '💡', '상식'], ['kpop', '🎤', 'K-POP'], ['movie', '🎬', '영화·드라마'], ['history', '🏛️', '역사'], ['meme', '🤣', '밈·웹툰'], ['nonsense', '🧩', '넌센스']];
// 마왕의 심판에 나오는 주관식 문제 (주제별 5문제). a = 인정하는 답 목록 — 비교할 때는 띄어쓰기/대소문자/기호를 모두 무시하므로 영어로 써도 되는 것은 영어 표기도 같이 적어 둠
const SUBJ_BANK = [{"t": "clock", "q": "롤렉스 로고에 그려진 모양은? (두 글자)", "a": ["왕관", "크라운", "crown"]}, {"t": "clock", "q": "1969년 달에 간 시계로 유명해진 오메가의 모델 이름은?", "a": ["스피드마스터", "speedmaster"]}, {"t": "clock", "q": "2015년에 처음 나온 애플의 스마트워치 이름은?", "a": ["애플워치", "applewatch"]}, {"t": "clock", "q": "시계에서 숫자나 눈금이 그려진 '시계 얼굴' 부분을 영어로 뭐라고 할까요? (한글로 써도 돼요)", "a": ["다이얼", "dial"]}, {"t": "clock", "q": "튼튼하기로 유명한 'G-SHOCK' 시계를 만든 일본 회사 이름은?", "a": ["카시오", "casio"]}, {"t": "game", "q": "슈퍼 마리오가 구해야 하는 공주의 이름은?", "a": ["피치", "피치공주", "peach", "princesspeach"]}, {"t": "game", "q": "포켓몬스터 피카츄의 타입은?", "a": ["전기", "전기타입", "electric"]}, {"t": "game", "q": "마인크래프트에서 다가와서 '쉬익' 소리를 내며 폭발하는 초록색 몬스터 이름은?", "a": ["크리퍼", "creeper"]}, {"t": "game", "q": "리그 오브 레전드의 기본 5대5 맵 이름은?", "a": ["소환사의협곡", "소환사협곡", "summonersrift"]}, {"t": "game", "q": "『젤다의 전설』에서 플레이어가 조작하는 초록 옷 주인공의 이름은?", "a": ["링크", "link"]}, {"t": "anime", "q": "『원피스』에서 루피가 먹은 악마의 열매 이름은?", "a": ["고무고무열매", "고무고무", "gomugomu", "gumgum"]}, {"t": "anime", "q": "『나루토』에서 나루토의 몸속에 봉인된 꼬리 아홉 달린 여우의 이름은?", "a": ["쿠라마", "kurama"]}, {"t": "anime", "q": "『귀멸의 칼날』 주인공의 이름은? (이름만 써도 돼요)", "a": ["탄지로", "카마도탄지로", "tanjiro", "kamadotanjiro"]}, {"t": "anime", "q": "『드래곤볼』에서 손오공이 사이어인으로 태어났을 때의 이름은?", "a": ["카카로트", "kakarot"]}, {"t": "anime", "q": "『이웃집 토토로』에서 사츠키의 어린 여동생 이름은?", "a": ["메이", "mei"]}, {"t": "general", "q": "태양계에서 가장 큰 행성은?", "a": ["목성", "jupiter"]}, {"t": "general", "q": "물의 화학식은?", "a": ["h2o", "에이치투오"]}, {"t": "general", "q": "해발 높이 기준으로 세계에서 가장 높은 산은?", "a": ["에베레스트", "에베레스트산", "everest", "mounteverest", "초모랑마"]}, {"t": "general", "q": "사람 몸에서 가장 큰 장기는?", "a": ["피부", "skin"]}, {"t": "general", "q": "지구에서 가장 넓은 바다(대양)는?", "a": ["태평양", "pacific", "pacificocean"]}, {"t": "kpop", "q": "방탄소년단 멤버 RM의 예전 활동명은?", "a": ["랩몬스터", "rapmonster"]}, {"t": "kpop", "q": "블랙핑크가 2016년 데뷔 싱글에서 'Whistle(휘파람)'과 함께 낸 곡은?", "a": ["붐바야", "boombayah"]}, {"t": "kpop", "q": "싸이가 2012년에 낸 전 세계적 히트곡은?", "a": ["강남스타일", "gangnamstyle"]}, {"t": "kpop", "q": "소녀시대의 데뷔곡 제목은?", "a": ["다시만난세계", "다만세", "intothenewworld"]}, {"t": "kpop", "q": "뉴진스가 속한 소속사(레이블) 이름은?", "a": ["어도어", "ador"]}, {"t": "movie", "q": "영화 『기생충』에서 박 사장네 가정부의 이름은?", "a": ["문광", "이정은"]}, {"t": "movie", "q": "『오징어 게임』에서 456번 참가자의 이름은?", "a": ["성기훈", "기훈"]}, {"t": "movie", "q": "『해리 포터』 주인공의 이마에 있는 흉터는 어떤 모양일까요?", "a": ["번개", "번개모양", "lightning", "lightningbolt"]}, {"t": "movie", "q": "마블 『어벤져스』에서 타노스가 모으려고 한 여섯 개의 보석 이름은?", "a": ["인피니티스톤", "인피니티스톤즈", "infinitystone", "infinitystones", "인피니티젬", "infinitygem"]}, {"t": "movie", "q": "『겨울왕국』에서 엘사가 만든 말하는 눈사람의 이름은?", "a": ["올라프", "olaf"]}, {"t": "history", "q": "이순신 장군이 학익진으로 일본 수군을 크게 이긴 1592년 해전은?", "a": ["한산도대첩", "한산대첩", "한산도", "hansando"]}, {"t": "history", "q": "훈민정음(한글)을 만든 조선의 왕은?", "a": ["세종", "세종대왕", "sejong"]}, {"t": "history", "q": "고구려를 세운 시조의 이름은?", "a": ["주몽", "고주몽", "동명성왕", "동명왕"]}, {"t": "history", "q": "1945년 8월 15일 우리나라가 일제에서 벗어난 것을 뭐라고 부를까요?", "a": ["광복", "광복절", "해방", "8.15광복"]}, {"t": "history", "q": "조선을 세운 첫 번째 왕(태조)의 본래 이름은?", "a": ["이성계", "성계", "태조이성계"]}, {"t": "meme", "q": "영화 『살인의 추억』에서 \"밥은 먹고 다니냐?\"라고 묻는 형사의 이름은?", "a": ["박두만", "두만"]}, {"t": "meme", "q": "웹툰 『신의 탑』의 주인공 이름은?", "a": ["스물다섯번째밤", "밤", "baam", "bam"]}, {"t": "meme", "q": "웹툰 『나 혼자만 레벨업』의 주인공 이름은?", "a": ["성진우", "진우", "sungjinwoo", "jinwoo"]}, {"t": "meme", "q": "'중꺾마'를 풀어 쓰면 '중요한 것은 꺾이지 않는 ○○'입니다. ○○에 들어갈 두 글자는?", "a": ["마음"]}, {"t": "meme", "q": "웹툰 『마음의 소리』를 그린 작가의 이름은?", "a": ["조석"]}];
const normAns = (x) => String(x || '').toLowerCase().replace(/[^0-9a-z\u3131-\u318e\uac00-\ud7a3]/g, '');
const SUBJ_OK = SUBJ_BANK.map((q) => new Set(q.a.map(normAns)));
const QUIZ_TOPICS = Object.fromEntries(QUIZ_TOPIC_LIST.map(([id, , name]) => [id, name]));
const QUIZ_TOPIC_INFO = QUIZ_TOPIC_LIST.map(([id, emoji, name]) => ({ id, emoji, name }));
const QUIZ_N = 1, QUIZ_INVITE_MS = 60000, QUIZ_ASK_MS = 20000, QUIZ_REVEAL_MS = 5000, QUIZ_GAIN = 10;
const quizRun = new Map();                                    // code -> {phase: idle|invite|ask|reveal, nextAt, id, topic, order, i, resp, players, answers, score, until}
const quizOf = (code) => { let r = quizRun.get(code); if (!r) { r = { phase: 'idle', nextAt: 0 }; quizRun.set(code, r); } return r; };
const quizCfg = (g) => g.quiz || (g.quiz = { on: false, everyMin: 60 });
function quizCfgFrom(m, old) {
  const on = typeof m.on === 'boolean' ? m.on : old.on, every = Math.round(Number(m.everyMin === undefined ? old.everyMin : m.everyMin));
  if (!(every >= 1 && every <= 1440)) fail('퀴즈 주기는 1분에서 24시간 사이로 정해 주세요.');
  return { on, everyMin: every };
}
const liveUids = (code) => new Set([...(rooms.get(code) || new Map()).values()].map((w) => w.uid));
function sendUids(code, uids, msg) { for (const w of (rooms.get(code) || new Map()).values()) if (uids.has(w.uid)) send(w, msg); }
const alivePlayers = (code, r) => { const live = liveUids(code); return [...r.players.keys()].filter((u) => live.has(u)); };
function startInvite(code, g) {
  const r = quizOf(code), now = Date.now();
  Object.assign(r, { phase: 'invite', id: crypto.randomBytes(4).toString('hex'), topic: null, votes: new Map(), order: [], i: 0, resp: new Map(), players: new Map(), answers: new Map(), score: new Map(), until: now + QUIZ_INVITE_MS });
  broadcast(code, quizInvite(r, now));
}
const quizInvite = (r, now) => ({ t: 'quiz', id: r.id, topics: QUIZ_TOPIC_INFO, n: QUIZ_N, until: r.until, now });
function quizTally(r) { const tally = {}; for (const [uid, tp] of r.votes) if (r.resp.get(uid)) tally[tp] = (tally[tp] || 0) + 1; return tally; }
function quizProg(code, r) { let yes = 0, no = 0; for (const v of r.resp.values()) v ? yes++ : no++; broadcast(code, { t: 'qprog', id: r.id, yes, no, total: liveUids(code).size, tally: quizTally(r) }); }
function quizPick(g, topic) {   // 그 그룹이 최근에 푼 문제는 빼고 고른다. 다 풀었으면 그 주제 기록을 비우고 다시 시작
  const seen = (g.quizSeen = g.quizSeen || {}), all = QUIZ_BANK.map((q, i) => i).filter((i) => QUIZ_BANK[i].t === topic);
  let pool = all.filter((i) => !(seen[topic] || []).includes(i)); if (pool.length < QUIZ_N) { seen[topic] = []; pool = all; }
  for (let i = pool.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [pool[i], pool[j]] = [pool[j], pool[i]]; }
  const pick = pool.slice(0, QUIZ_N); seen[topic] = (seen[topic] || []).concat(pick); return pick;
}
function startQuestions(code, g) {
  const r = quizOf(code), live = liveUids(code);
  for (const [uid, yes] of r.resp) if (yes && live.has(uid)) { const mem = memberOf(g, uid); if (mem) r.players.set(uid, mem.name); }
  if (!r.players.size) return endQuiz(code, g, 'noone');
  const tally = {}; for (const uid of r.players.keys()) { const tp = r.votes.get(uid); if (QUIZ_TOPICS[tp]) tally[tp] = (tally[tp] || 0) + 1; }
  const ids = Object.keys(QUIZ_TOPICS), voted = Object.keys(tally), cand = voted.length ? voted : ids, w = cand.map((t) => tally[t] || 1), sum = w.reduce((x, y) => x + y, 0);
  let x = crypto.randomInt(sum), k = 0; while (x >= w[k]) { x -= w[k]; k++; }
  r.topic = cand[k]; r.order = quizPick(g, r.topic); persist();
  const pct = {}; if (voted.length) for (const t of voted) pct[t] = Math.round((tally[t] / sum) * 100);
  broadcast(code, { t: 'qstart', id: r.id, topic: r.topic, topicName: QUIZ_TOPICS[r.topic], n: QUIZ_N, tally, pct, players: [...r.players].map(([uid, name]) => ({ uid, name })) });
  askQ(code, g);
}
function askQ(code, g) {
  const r = quizOf(code), now = Date.now(), q = QUIZ_BANK[r.order[r.i]]; r.phase = 'ask'; r.answers = new Map(); r.until = now + QUIZ_ASK_MS;
  sendUids(code, new Set(r.players.keys()), { t: 'qq', id: r.id, i: r.i, n: QUIZ_N, topicName: QUIZ_TOPICS[r.topic], text: q.q, choices: q.c, until: r.until, now });
}
function resolveQ(code, g) {
  const r = quizOf(code); if (r.phase !== 'ask') return; const q = QUIZ_BANK[r.order[r.i]], right = [], counts = [0, 0, 0, 0]; g.xp = g.xp || {};
  for (const [uid, c] of r.answers) {
    counts[c]++; if (c !== q.a) continue;
    g.xp[uid] = Math.min(MAX_XP, (g.xp[uid] || 0) + QUIZ_GAIN); const sc = r.score.get(uid) || { right: 0, gain: 0 }; sc.right++; sc.gain += QUIZ_GAIN; r.score.set(uid, sc);
    right.push({ uid, name: r.players.get(uid), gain: QUIZ_GAIN, xp: g.xp[uid] });
  }
  if (right.length) persist();
  r.phase = 'reveal'; r.until = Date.now() + QUIZ_REVEAL_MS;
  sendUids(code, new Set(r.players.keys()), { t: 'qres', id: r.id, i: r.i, n: QUIZ_N, correct: q.a, right, counts, answered: r.answers.size });
}
function endQuiz(code, g, reason) {
  const r = quizOf(code), cfg = quizCfg(g), now = Date.now();
  const board = [...r.players].map(([uid, name]) => { const sc = r.score.get(uid) || { right: 0, gain: 0 }; return { uid, name, right: sc.right, gain: sc.gain, xp: (g.xp || {})[uid] || 0 }; }).sort((a, b) => b.right - a.right);
  const msg = { t: 'qend', id: r.id, reason, topicName: QUIZ_TOPICS[r.topic] || '', n: QUIZ_N, board, players: [...r.players.keys()] };
  r.phase = 'idle'; r.nextAt = now + cfg.everyMin * 60000; if (r.id) broadcast(code, msg);
}
function quizTick(code, g, now) {
  const r = quizRun.get(code), cfg = g.quiz;
  if (r && r.phase !== 'idle') {
    if (!cfg || !cfg.on) return endQuiz(code, g, 'off');
    if (r.phase === 'invite' && now >= r.until) startQuestions(code, g);
    else if (r.phase === 'ask') { const alive = alivePlayers(code, r); if (!alive.length) endQuiz(code, g, 'gone'); else if (now >= r.until || alive.every((u) => r.answers.has(u))) resolveQ(code, g); }
    else if (r.phase === 'reveal' && now >= r.until) { r.i++; if (r.i >= QUIZ_N) endQuiz(code, g, 'done'); else askQ(code, g); }
    return;
  }
  if (!cfg || !cfg.on) return; const rr = quizOf(code);
  if (now >= rr.nextAt) { if (online(code)) startInvite(code, g); else rr.nextAt = now + cfg.everyMin * 60000; }
}
setInterval(() => { const now = Date.now(); for (const [code, g] of groups) { try { quizTick(code, g, now); } catch (e) { console.error('quiz', e); } } }, 1000);

const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  for (;;) {
    const b = crypto.randomBytes(12); let s = '';
    for (let i = 0; i < 12; i++) s += ALPHA[b[i] % ALPHA.length];
    const c = s.slice(0, 8) + '-' + s.slice(8);
    if (!groups.has(c)) return c;
  }
}
const normCode = (s) => { const x = String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); return x.length === 12 ? x.slice(0, 8) + '-' + x.slice(8) : x; };

class Fail extends Error {}
const fail = (m) => { throw new Fail(m); };
const uidOf = (m) => /^[A-Za-z0-9-]{16,64}$/.test(m.uid || '') ? m.uid : fail('잘못된 요청이에요.');
const nameOf = (s) => { const a = [...String(s || '').trim()]; return a.length >= 2 && a.length <= 8 ? a.join('') : fail('닉네임은 2~8자로 입력해 주세요.'); };
const lookOf = (n) => Number.isInteger(n) && n >= 0 && n < 100 ? n : 0;
const gnameOf = (s) => { const a = [...String(s || '').trim()]; return a.length >= 1 && a.length <= 20 ? a.join('') : fail('그룹 이름은 1~20자로 입력해 주세요.'); };
const getGroup = (m) => { const code = normCode(m.code); return [code, groups.get(code) || fail('그룹을 찾을 수 없어요.')]; };
const memberOf = (g, uid) => g.members.find((x) => x.uid === uid);

function view(code, g, uid) {
  const live = new Map([...(rooms.get(code) || new Map()).values()].map((w) => [w.uid, w.state]));   // uid -> 'online' | 'away'
  const cfg = bossCfg(g), r = bossRun.get(code), now = Date.now();
  return { code, name: g.name, owner: g.owner === uid, max: MAX, xp: (g.xp || {})[uid] || 0, lordKills: (g.lordKills || {})[uid] || 0,
    fx: fxPublic(g, code),
    quiz: { ...quizCfg(g), active: !!(quizRun.get(code) && quizRun.get(code).phase !== 'idle'), nextInMs: quizCfg(g).on ? Math.max(0, ((quizRun.get(code) || {}).nextAt || 0) - now) : null },
    boss: { ...cfg, lordReady: lordReady(code), lordNeed: LORD_NEED_N, lordLv: LORD_NEED_LV, active: r && r.cur ? { type: r.cur.type, leftMs: Math.max(0, r.cur.until - now) } : null, nextInMs: cfg.on && !(r && r.cur) ? Math.max(0, ((r && r.nextAt) || 0) - now) : null },
    members: g.members.map((m) => ({ uid: m.uid, name: m.name, look: m.look, owner: m.uid === g.owner, me: m.uid === uid, online: live.has(m.uid), state: live.get(m.uid) || 'offline' })) };
}

// ---- 뿌셔뿌셔 존 / 외침 단어 (방장이 실시간으로 정함) ----
// smash: 물건이 떨어지는 뿌셔뿌셔 존 ON/OFF, rate: 속도 게이지 0(느림, 4.5초에 1개)~100(최대, 0.1초에 1개), t0: 물건이 떨어지기 시작한 기준 시각(모두 같은 물건이 같은 때 떨어지게)
// words: 이 단어가 들어간 채팅은 삐죽삐죽 흔들리는 말풍선으로 보임
const fxCfg = (g) => g.fx || (g.fx = { smash: false, rate: 25, t0: 0, words: [] });
function fxCfgFrom(m, old) {
  const out = { smash: typeof m.smash === 'boolean' ? m.smash : old.smash, rate: old.rate === undefined ? 25 : old.rate, t0: old.t0, words: old.words };
  if (m.rate !== undefined) { const d = Math.round(Number(m.rate)); if (!(d >= 0 && d <= 100)) fail('속도는 0~100 으로 정해 주세요.'); out.rate = d; }
  if (m.words !== undefined) {
    if (!Array.isArray(m.words)) fail('단어 목록이 잘못됐어요.');
    const ws = []; for (const w of m.words) { const t = String(w || '').trim(); if (!t) continue; if ([...t].length > 12) fail('단어는 12자까지 정할 수 있어요.'); if (!ws.includes(t)) ws.push(t); }
    if (ws.length > 30) fail('단어는 30개까지 정할 수 있어요.'); out.words = ws;
  }
  if (out.smash && (!old.smash || out.rate !== (old.rate === undefined ? 25 : old.rate))) out.t0 = Date.now();
  return out;
}
// 상사 봉제인형: 뿌셔뿌셔 존이 켜질 때 1~2개(남자상사 m / 여자상사 f)가 나오고, 체력 50 을 친구들이 같이 깎는다 (서버가 체력을 기억; 서버를 다시 켜면 새로 나옴)
const dollRun = new Map();                                    // code -> { list: [{id:'m'|'f', hp, x(0~1 가로 위치)}] }
const DOLL_HP = 50, rnd1000 = () => crypto.randomInt(1000) / 1000;
function spawnDolls(code) {
  const two = crypto.randomInt(2) === 1;
  const list = two ? [{ id: 'm', hp: DOLL_HP, x: 0.1 + rnd1000() * 0.3 }, { id: 'f', hp: DOLL_HP, x: 0.6 + rnd1000() * 0.3 }] : [{ id: crypto.randomInt(2) ? 'm' : 'f', hp: DOLL_HP, x: 0.2 + rnd1000() * 0.6 }];
  dollRun.set(code, { list });
}
const dollsPublic = (code) => ((dollRun.get(code) || {}).list || []).map((d) => ({ id: d.id, hp: d.hp, x: Math.round(d.x * 1000) / 1000 }));
const fxPublic = (g, code) => { const f = fxCfg(g); return { smash: !!f.smash, rate: f.rate === undefined ? 25 : f.rate, t0: f.t0, words: f.words, dolls: f.smash ? dollsPublic(code) : [] }; };

const ops = {
  fxcfg: (m) => {                             // 방장만: 뿌셔뿌셔 존 / 외침 단어. 바꾸면 접속 중인 모두에게 바로 적용
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    const was = fxCfg(g).smash; g.fx = fxCfgFrom(m, fxCfg(g)); persist();
    if (g.fx.smash && !was) spawnDolls(code); else if (!g.fx.smash) dollRun.delete(code);
    broadcast(code, { t: 'fxcfg', ...fxPublic(g, code) }); return fxPublic(g, code);
  },
  list: (m) => { const uid = uidOf(m); return [...groups].filter(([, g]) => memberOf(g, uid)).map(([c, g]) => view(c, g, uid)); },
  create: (m) => {
    const uid = uidOf(m), name = nameOf(m.name), gn = gnameOf(m.groupName);
    if ([...groups.values()].filter((g) => memberOf(g, uid)).length >= MAX_GROUPS) fail(`그룹은 최대 ${MAX_GROUPS}개까지 참여할 수 있어요.`);
    const code = newCode();
    groups.set(code, { name: gn, owner: uid, members: [{ uid, name, look: lookOf(m.look) }] });
    persist(); return { code };
  },
  join: (m) => {
    const uid = uidOf(m), name = nameOf(m.name), code = normCode(m.code), g = groups.get(code);
    if (!g) fail('초대 코드를 확인해 주세요.');
    if (memberOf(g, uid)) return { code };
    if (g.members.length >= MAX) fail('그룹이 가득 찼어요. (최대 12명)');
    if ([...groups.values()].filter((x) => memberOf(x, uid)).length >= MAX_GROUPS) fail(`그룹은 최대 ${MAX_GROUPS}개까지 참여할 수 있어요.`);
    g.members.push({ uid, name, look: lookOf(m.look) });
    broadcast(code, { t: 'member', uid, name, look: lookOf(m.look) });
    persist(); return { code };
  },
  leave: (m) => {
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (!memberOf(g, uid)) fail('참여 중인 그룹이 아니에요.');
    g.members = g.members.filter((x) => x.uid !== uid);
    broadcast(code, { t: 'gone', uid });
    drop(code, (w) => w.uid === uid, 'kicked', '그룹에서 나갔어요.');
    if (!g.members.length) groups.delete(code); else if (g.owner === uid) g.owner = g.members[0].uid; // 가장 오래된 멤버가 방장
    persist(); return {};
  },
  kick: (m) => {
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    if (m.target === uid || !memberOf(g, m.target)) fail('내보낼 멤버를 찾을 수 없어요.');
    g.members = g.members.filter((x) => x.uid !== m.target);
    broadcast(code, { t: 'gone', uid: m.target });
    drop(code, (w) => w.uid === m.target, 'kicked', '그룹에서 제외됐어요.');
    persist(); return {};
  },
  rename: (m) => {
    const uid = uidOf(m), [, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    g.name = gnameOf(m.groupName); persist(); return {};
  },
  delete: (m) => {
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    drop(code, () => true, 'kicked', '그룹이 삭제됐어요.');
    groups.delete(code); bossRun.delete(code); quizRun.delete(code); msgLog.delete(code); persistMsgs(); persist(); return {};
  },
  bosscfg: (m) => {                           // 방장만: 보스 ON/OFF, 출현 주기(분), 머무는 시간(분)
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    const old = bossCfg(g), cfg = cfgFrom(m, old), r = runOf(code);
    g.boss = cfg; if (cfg.on && (!old.on || cfg.everyMin !== old.everyMin)) r.nextAt = Date.now() + cfg.everyMin * 60000;   // 새로 켜거나 주기를 바꾸면 그때부터 다시 센다
    if (!cfg.on && r.cur) endBoss(code, g, 'off');
    persist(); broadcast(code, { t: 'bosscfg', boss: cfg }); return view(code, g, uid).boss;
  },
  bossnow: (m) => {                           // 방장만: 지금 바로 보스 소환 (켜져 있고 보스가 없을 때)
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    if (!bossCfg(g).on) fail('보스가 꺼져 있어요. 먼저 켜 주세요.');
    if (runOf(code).cur) fail('이미 보스가 있어요.');
    if (!online(code)) fail('접속 중인 멤버가 없어서 소환할 수 없어요.');
    if (m.lord) { if (lordReady(code) < LORD_NEED_N) fail(`Lv${LORD_NEED_LV} 이상이 ${LORD_NEED_N}명 이상 접속해 있어야 마왕이 나와요.`); spawnBoss(code, g, 'lord'); } else spawnBoss(code, g);
    return {};
  },
  updnotify: (m) => {                         // 방장만: 접속 중인 그룹원 앱들에게 "새 버전 확인해 보세요" 신호 (1분에 한 번)
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    const now = Date.now(), r = runOf(code); if (now - (r.lastUpd || 0) < 60000) fail('방금 보냈어요. 1분 뒤에 다시 보낼 수 있어요.');
    r.lastUpd = now; const sent = online(code); broadcast(code, { t: 'updnow' }); return { sent };
  },
  quizcfg: (m) => {                           // 방장만: 랜덤퀴즈 ON/OFF, 주기(분)
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    const old = quizCfg(g), cfg = quizCfgFrom(m, old), r = quizOf(code);
    g.quiz = cfg; if (cfg.on && (!old.on || cfg.everyMin !== old.everyMin) && r.phase === 'idle') r.nextAt = Date.now() + cfg.everyMin * 60000;
    persist(); return view(code, g, uid).quiz;
  },
  quiznow: (m) => {                           // 방장만: 지금 바로 퀴즈 시작 (켜져 있고 진행 중이 아닐 때)
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (g.owner !== uid) fail('방장만 할 수 있어요.');
    if (!quizCfg(g).on) fail('랜덤퀴즈가 꺼져 있어요. 먼저 켜 주세요.');
    if (quizOf(code).phase !== 'idle') fail('이미 퀴즈가 진행 중이에요.');
    if (!online(code)) fail('접속 중인 멤버가 없어서 시작할 수 없어요.');
    startInvite(code, g); return {};
  },
  status: () => ({ empty: groups.size === 0, retainMs: RETAIN_MS }),
  restore: (m) => {                           // 서버 데이터가 사라졌을 때 멤버의 PC에 남은 정보로 같은 초대코드의 그룹을 다시 만든다
    const uid = uidOf(m), code = normCode(m.code);
    if (!/^[A-HJ-NP-Z2-9]{8}-[A-HJ-NP-Z2-9]{4}$/.test(code)) fail('잘못된 그룹 정보예요.');
    if (groups.has(code)) return { code, existed: true };
    const seen = new Set(), members = [];
    for (const x of (Array.isArray(m.members) ? m.members : []).slice(0, MAX)) {
      try { if (!/^[A-Za-z0-9-]{16,64}$/.test(x.uid || '') || seen.has(x.uid)) continue; members.push({ uid: x.uid, name: nameOf(x.name), look: lookOf(x.look) }); seen.add(x.uid); } catch {}
    }
    if (!seen.has(uid)) fail('복원할 수 없는 그룹이에요.');
    const grp = { name: gnameOf(m.groupName), owner: seen.has(m.owner) ? m.owner : members[0].uid, members };
    try { if (m.boss && typeof m.boss === 'object') grp.boss = cfgFrom(m.boss, { on: false, everyMin: 60, lastsMin: 10 }); } catch {}
    try { if (m.quiz && typeof m.quiz === 'object') grp.quiz = quizCfgFrom(m.quiz, { on: false, everyMin: 60 }); } catch {}
    try { if (m.fx && typeof m.fx === 'object') grp.fx = fxCfgFrom(m.fx, { smash: false, rate: 25, t0: 0, words: [] }); } catch {}
    groups.set(code, grp);
    persist(); return { code, restored: true };
  },
  history: (m) => {                         // 최근 기록: {before}=그보다 오래된 것, {after}=그보다 새로운 것, 둘 다 없으면 가장 최근
    const uid = uidOf(m), [code, g] = getGroup(m);
    if (!memberOf(g, uid)) fail('참여 중인 그룹이 아니에요.');
    const cut = Date.now() - RETAIN_MS, lim = Math.min(100, Math.max(1, Number(m.limit) || 50));
    let list = (msgLog.get(code) || []).filter((x) => x.ts >= cut), more = false;
    if (typeof m.after === 'number') list = list.filter((x) => x.ts > m.after).slice(0, 200);
    else { if (typeof m.before === 'number') list = list.filter((x) => x.ts < m.before); more = list.length > lim; list = list.slice(-lim); }
    const names = new Map(g.members.map((x) => [x.uid, x.name]));
    return { groupName: g.name, more, retainMs: RETAIN_MS, messages: list.map((x) => ({ id: x.id, ts: x.ts, uid: x.uid, name: names.get(x.uid) || x.name, text: x.text, me: x.uid === uid })) };
  },
  profile: (m) => {
    const uid = uidOf(m), name = nameOf(m.name), look = lookOf(m.look);
    for (const [code, g] of groups) {
      const mem = memberOf(g, uid); if (!mem) continue;
      mem.name = name; mem.look = look;
      broadcast(code, { t: 'peerupdate', uid, name, look });
    }
    persist(); return {};
  },
};

const server = require('http').createServer((req, res) => {          // 호스팅 서비스의 상태 확인용 (WebSocket 이 아닌 일반 요청)
  res.writeHead(req.url === '/' || req.url === '/healthz' ? 200 : 404, { 'Content-Type': 'text/plain' });
  res.end(req.url === '/' || req.url === '/healthz' ? 'ok' : '');
});
const wss = new WebSocketServer({ server, maxPayload: 2048 });
wss.on('connection', (ws) => {
  ws.id = String(nextId++); ws.room = null; ws.uid = null; ws.alive = true; ws.lastChat = 0; ws.lastPulse = 0; ws.lastBhit = 0; ws.lastBthrow = 0; ws.state = 'online';
  ws.on('pong', () => (ws.alive = true));

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.t === 'rpc') {
      try { send(ws, { t: 'rpc', rid: m.rid, ok: true, data: (ops[m.op] || (() => fail('지원하지 않는 요청이에요.')))(m) }); }
      catch (e) { if (!(e instanceof Fail)) console.error(e); send(ws, { t: 'rpc', rid: m.rid, ok: false, msg: e instanceof Fail ? e.message : '서버 오류가 발생했어요.' }); }
      return;
    }

    if (m.t === 'join' && !ws.room) {
      const code = normCode(m.code), g = groups.get(code), mem = g && /^[A-Za-z0-9-]{16,64}$/.test(m.uid || '') && memberOf(g, m.uid);
      if (!mem) { send(ws, { t: 'err', msg: '이 그룹에 참여 중이 아니에요. 설정에서 그룹을 확인해 주세요.' }); return ws.close(); }
      const r = rooms.get(code) || new Map();
      for (const w of [...r.values()]) if (w.uid === m.uid) { send(w, { t: 'kicked', msg: '다른 곳에서 접속했어요.' }); w.close(); }
      const live = new Set([...r.values()].map((w) => w.uid));
      const peers = [...r.values()].filter((w) => w.uid !== m.uid).map((w) => { const x = memberOf(g, w.uid); return { id: w.id, uid: w.uid, name: x.name, look: x.look, state: w.state }; });
      const offline = g.members.filter((x) => x.uid !== m.uid && !live.has(x.uid)).map((x) => ({ uid: x.uid, name: x.name, look: x.look }));
      r.set(ws.id, ws); rooms.set(code, r); ws.room = code; ws.uid = m.uid; ws.lastBhit = 0;
      g.xp = g.xp || {}; const mine = Math.min(MAX_XP, Math.max(0, Math.floor(Number(m.xp) || 0))); if (mine > (g.xp[m.uid] || 0)) { g.xp[m.uid] = mine; persist(); }   // 서버 데이터가 사라졌어도 내 PC 에 적어 둔 경험치로 되살림(큰 쪽을 씀)
      { g.lordKills = g.lordKills || {}; const lk = Math.min(999, Math.max(0, Math.floor(Number(m.lk) || 0))); if (lk > (g.lordKills[m.uid] || 0)) { g.lordKills[m.uid] = lk; persist(); } }   // 마왕 처치 횟수도 내 PC 기록과 비교해 큰 쪽
      if (g.fx && g.fx.smash && !dollRun.has(code)) spawnDolls(code);
      const run = bossRun.get(code), now = Date.now();
      send(ws, { t: 'welcome', id: ws.id, peers, offline, xps: g.xp, lks: g.lordKills || {}, boss: bossPublic(run && run.cur, now), bosscfg: bossCfg(g), fx: fxPublic(g, code) });
      { const qr = quizRun.get(code); if (qr && qr.phase === 'invite') { send(ws, quizInvite(qr, now)); quizProg(code, qr); } }
      broadcast(code, { t: 'peer', id: ws.id, uid: m.uid, name: mem.name, look: mem.look, state: 'online', xp: g.xp[m.uid] || 0 }, ws.id);
      return;
    }
    if (!ws.room) return;

    if (m.t === 'presence' && (m.state === 'away' || m.state === 'online')) { ws.state = m.state; broadcast(ws.room, { t: 'presence', id: ws.id, state: m.state }, ws.id); }
    else if (m.t === 'typing') broadcast(ws.room, { t: 'typing', id: ws.id, on: !!m.on }, ws.id);
    else if (m.t === 'pulse') { const now = Date.now(); if (now - ws.lastPulse < 900) return; ws.lastPulse = now; broadcast(ws.room, { t: 'pulse', id: ws.id }, ws.id); }   // 크기 효과: 1초에 한 번
    else if (m.t === 'pos' && typeof m.x === 'number') {                // 위치 + (보내는 쪽이 알려 준) 걷는 중 여부 w, 방향 d. 예전 앱처럼 w 가 없으면 그대로 생략
      const out = { t: 'pos', id: ws.id, x: Math.min(1, Math.max(0, m.x)) };
      if (m.w !== undefined) { out.w = m.w ? 1 : 0; out.d = m.d === -1 ? -1 : 1; }
      if (typeof m.e === 'string') out.e = m.e.replace(/[^\w-]/g, '').slice(0, 12);                 // 들고 있는(고른) 던지기 아이템 id (총을 들고 있는 모습을 모두에게 보여줌)
      broadcast(ws.room, out, ws.id);
    }
    else if (m.t === 'chat') {
      const now = Date.now(); if (now - ws.lastChat < 300) return; ws.lastChat = now;
      const text = String(m.text || '').trim().slice(0, 200);
      if (text) {
        broadcast(ws.room, { t: 'chat', id: ws.id, text, bubble: String(m.bubble || '').slice(0, 16) });
        const g = groups.get(ws.room), mem = g && memberOf(g, ws.uid); if (mem) pushMsg(ws.room, ws.uid, mem.name, text);
      }
    } else if (m.t === 'sthrow') {                                      // 뿌셔뿌셔: 내가 k 번째 물건에 무기를 쐈다 -> 다른 사람 화면에도 같이 보이게 전달
      const g = groups.get(ws.room), now = Date.now(), k = Number(m.k), d = m.d === 'm' || m.d === 'f' ? m.d : null; if (!g || !g.fx || !g.fx.smash || (!d && !(Number.isInteger(k) && k >= 0)) || now - (ws.lastSthrow || 0) < 100) return; ws.lastSthrow = now;
      broadcast(ws.room, { t: 'sthrow', from: ws.id, k: d ? undefined : k, d, item: String(m.item || '').slice(0, 16) }, ws.id);
    } else if (m.t === 'dhit') {                                        // 상사 인형을 때림: 체력은 서버가 깎고 모두에게 알림 (한 번에 최대 2마리, 0.09초에 한 번)
      const g = groups.get(ws.room), run = dollRun.get(ws.room), now = Date.now(); if (!g || !g.fx || !g.fx.smash || !run || now - (ws.lastDhit || 0) < 90) return; ws.lastDhit = now;
      for (const h of (Array.isArray(m.hits) ? m.hits : []).slice(0, 2)) {
        const d = run.list.find((x) => x.id === (h && h.d)); if (!d || d.hp <= 0) continue;
        const dmg = Math.min(d.hp, Math.min(20, Math.max(1, Math.floor(Number(h.dmg) || 1)))); d.hp -= dmg;
        broadcast(ws.room, { t: 'dhp', d: d.id, hp: d.hp, by: ws.id, dmg });
      }
    } else if (m.t === 'smash') {                                       // 뿌셔뿌셔: 내 무기로 부순 물건 번호들(폭발이면 여러 개) -> 다른 사람 화면에서도 같은 물건이 부서짐
      const g = groups.get(ws.room), now = Date.now(); if (!g || !g.fx || !g.fx.smash || now - (ws.lastSmash || 0) < 40) return;
      const ks = (Array.isArray(m.k) ? m.k : [m.k]).slice(0, 40).map(Number).filter((k) => Number.isInteger(k) && k >= 0); if (!ks.length) return; ws.lastSmash = now;
      broadcast(ws.room, { t: 'smash', id: ws.id, k: ks }, ws.id);
    } else if (m.t === 'bthrow') {                                      // 보스에게 던지기: 모두에게 같이 보이게 전달 (피해는 bhit 로 따로)
      const run = bossRun.get(ws.room), now = Date.now(); if (!run || !run.cur || now >= run.cur.until || now - ws.lastBthrow < 150) return; ws.lastBthrow = now;
      broadcast(ws.room, { t: 'bthrow', from: ws.id, item: String(m.item || '').slice(0, 16) });
    } else if (m.t === 'bhit') {                                        // 공격이 보스에게 맞음: 맞춘 쪽 앱이 알려 줌. 피해 1~10 으로 제한, 연속 요청은 제한
      const run = bossRun.get(ws.room), now = Date.now(), g = groups.get(ws.room);
      if (!run || !run.cur || !g || run.cur.id !== String(m.id) || now >= run.cur.until || run.cur.hp <= 0 || now - ws.lastBhit < 120) return; ws.lastBhit = now;
      const lord = run.cur.type === 'lord'; if (lord && run.cur.judge) return;                     // 심판 중에는 피해 없음
      if (lord && Math.random() < LORD_EVADE) return broadcast(ws.room, { t: 'bmiss', id: run.cur.id, by: ws.id });   // 마왕은 20% 회피
      let dmg = Math.min(run.cur.hp, Math.min(20, Math.max(1, Math.floor(Number(m.dmg) || 1))));
      const step = lord ? LORD_STEPS[run.cur.th] : undefined; if (step !== undefined && run.cur.hp - dmg < step) dmg = run.cur.hp - step;   // 400/200 에서 멈춤
      run.cur.hp -= dmg; run.dmg.set(ws.uid, (run.dmg.get(ws.uid) || 0) + dmg); run.last = ws.uid;
      let heal = 0;                                                                              // 마왕은 공격이 성공할 때마다 피해의 일부를 회복 (이미 지나온 400/200 선 위로는 안 올라감)
      if (lord && run.cur.hp > 0 && !(step !== undefined && run.cur.hp <= step)) { const cap = run.cur.th > 0 ? LORD_STEPS[run.cur.th - 1] : run.cur.max; heal = Math.max(0, Math.min(Math.ceil(dmg * LORD_HEAL), cap - run.cur.hp)); run.cur.hp += heal; }
      broadcast(ws.room, { t: 'bhp', id: run.cur.id, hp: run.cur.hp, by: ws.id, dmg, heal });
      if (run.cur.hp <= 0) endBoss(ws.room, g, 'dead');
      else if (step !== undefined && run.cur.hp <= step) { run.cur.th++; startJudge(ws.room, g); }
    } else if (m.t === 'lans') {                                       // 마왕 문제 답 (심판받는 한 명만, 글자로 한 번)
      const run = bossRun.get(ws.room), g = groups.get(ws.room), j = run && run.cur && run.cur.judge, txt = String(m.text || '').slice(0, 40);
      if (!j || !g || run.cur.id !== String(m.id) || j.uid !== ws.uid || !normAns(txt)) return;
      if (Date.now() > j.until + 500) return judgeFail(ws.room, g, 'timeout');
      const q = SUBJ_BANK[j.qi];
      if (!SUBJ_OK[j.qi].has(normAns(txt))) return judgeFail(ws.room, g, 'wrong', txt);
      run.cur.until += Date.now() - j.start; run.cur.judge = null;                                // 심판 시간만큼 머무는 시간 연장
      broadcast(ws.room, { t: 'bjres', id: run.cur.id, ok: true, answer: q.a[0], picked: txt, until: run.cur.until });
    } else if (m.t === 'lhit') {                                       // 마왕 주먹에 내가 맞음(맞은 사람 앱이 판정) -> 모두에게 날아가고 기절하는 모습 전달
      const run = bossRun.get(ws.room), now = Date.now(); if (!run || !run.cur || run.cur.type !== 'lord' || run.cur.judge || run.cur.id !== String(m.id) || now - (ws.lastLhit || 0) < 1200) return; ws.lastLhit = now;
      broadcast(ws.room, { t: 'lhit', id: run.cur.id, who: ws.id, dir: m.dir === -1 ? -1 : 1 }, ws.id);
    } else if (m.t === 'qjoin') {                                      // 퀴즈 참여 수락/거절 (초대 중에만). 지금 접속한 사람이 모두 정하면 바로 시작
      const r = quizRun.get(ws.room), g = groups.get(ws.room); if (!r || !g || r.phase !== 'invite' || r.id !== String(m.id)) return;
      r.resp.set(ws.uid, !!m.yes);
      if (!m.yes) r.votes.delete(ws.uid); else if (typeof m.topic === 'string' && QUIZ_TOPICS[m.topic]) r.votes.set(ws.uid, m.topic);
      quizProg(ws.room, r);
      const live = liveUids(ws.room); if ([...live].every((u) => r.resp.has(u))) startQuestions(ws.room, g);
    } else if (m.t === 'qans') {                                       // 퀴즈 답 제출 (한 문제에 한 번만)
      const r = quizRun.get(ws.room), g = groups.get(ws.room), c = Number(m.c);
      if (!r || !g || r.phase !== 'ask' || r.id !== String(m.id) || r.i !== Number(m.i) || !r.players.has(ws.uid) || r.answers.has(ws.uid) || !(c >= 0 && c <= 3) || Date.now() > r.until + 500) return;
      r.answers.set(ws.uid, Math.floor(c));
      const alive = alivePlayers(ws.room, r); sendUids(ws.room, new Set(r.players.keys()), { t: 'qa', id: r.id, i: r.i, answered: r.answers.size, total: alive.length });
      if (alive.every((u) => r.answers.has(u))) resolveQ(ws.room, g);
    } else if (m.t === 'throw' && rooms.get(ws.room).has(String(m.to))) {
      broadcast(ws.room, { t: 'throw', from: ws.id, to: String(m.to), item: String(m.item || '').slice(0, 16) });
    }
  });

  ws.on('close', () => {
    const r = rooms.get(ws.room); if (!r) return;
    r.delete(ws.id);
    { const run = bossRun.get(ws.room), gg = groups.get(ws.room); if (run && run.cur && run.cur.judge && run.cur.judge.uid === ws.uid && gg) judgeFail(ws.room, gg, 'left'); }
    const g = groups.get(ws.room), member = !!(g && memberOf(g, ws.uid));
    if (!r.size) rooms.delete(ws.room); else broadcast(ws.room, { t: 'leave', id: ws.id, uid: ws.uid, member });
  });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 30000);
server.listen(PORT, '0.0.0.0', () => console.log(`pixel-pals server listening on :${PORT} (그룹 ${groups.size}개 로드, 채팅 ${RETAIN_MS / 3600000}시간 보관)`));
