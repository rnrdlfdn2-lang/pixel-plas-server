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
const bossRun = new Map();                                    // code -> {cur: 지금 보스, nextAt: 다음 출현 시각, dmg: Map(uid -> 준 피해), last: 마지막 일격 uid}   (저장하지 않는 실행 상태)
const runOf = (code) => { let r = bossRun.get(code); if (!r) { r = { cur: null, nextAt: 0, dmg: new Map(), last: null }; bossRun.set(code, r); } return r; };
const bossCfg = (g) => g.boss || (g.boss = { on: false, everyMin: 60, lastsMin: 10 });
const bossPublic = (b, now) => (b ? { id: b.id, type: b.type, seed: b.seed, t0: b.t0, until: b.until, hp: b.hp, max: BOSS_HP, now } : null);
function cfgFrom(m, old) {
  const on = typeof m.on === 'boolean' ? m.on : old.on, every = Math.round(Number(m.everyMin === undefined ? old.everyMin : m.everyMin)), lasts = Math.round(Number(m.lastsMin === undefined ? old.lastsMin : m.lastsMin));
  if (!(every >= 1 && every <= 1440)) fail('출현 주기는 1분에서 24시간 사이로 정해 주세요.');
  if (!(lasts >= 1 && lasts <= 180)) fail('머무는 시간은 1분에서 3시간 사이로 정해 주세요.');
  return { on, everyMin: every, lastsMin: lasts };
}
const online = (code) => (rooms.get(code) || new Map()).size;
function pickBoss() { let r = Math.random() * BOSS_TYPES.reduce((a, b) => a + b[1], 0); for (const b of BOSS_TYPES) { if ((r -= b[1]) < 0) return b; } return BOSS_TYPES[0]; }
function spawnBoss(code, g) {
  const r = runOf(code), cfg = bossCfg(g), now = Date.now(), [type] = pickBoss();
  r.cur = { id: crypto.randomBytes(4).toString('hex'), type, seed: crypto.randomInt(1, 2147483647), t0: now, until: now + cfg.lastsMin * 60000, hp: BOSS_HP };
  r.dmg = new Map(); r.last = null; r.nextAt = now + cfg.everyMin * 60000;
  broadcast(code, { t: 'boss', boss: bossPublic(r.cur, now) });
}
function endBoss(code, g, reason) {
  const r = runOf(code), b = r.cur; if (!b) return; r.cur = null;
  const now = Date.now(), cfg = bossCfg(g); r.nextAt = Math.max(b.t0 + cfg.everyMin * 60000, now + 5000);
  if (reason !== 'dead') return broadcast(code, { t: 'bossend', id: b.id, reason });
  const pool = (BOSS_TYPES.find((x) => x[0] === b.type) || BOSS_TYPES[0])[2], total = [...r.dmg.values()].reduce((a, c) => a + c, 0) || 1, rewards = [];
  g.xp = g.xp || {};
  for (const [uid, d] of r.dmg) {
    const mem = memberOf(g, uid); if (!mem) continue;
    const gain = Math.round(pool * (0.4 + 0.6 * (d / total)) + (uid === r.last ? pool * 0.15 : 0));       // 싸운 사람은 최소 40%, 준 피해 비율만큼 더, 마지막 일격은 +15%
    g.xp[uid] = Math.min(MAX_XP, (g.xp[uid] || 0) + gain); rewards.push({ uid, name: mem.name, dmg: d, gain, xp: g.xp[uid] });
  }
  rewards.sort((a, c) => c.dmg - a.dmg); persist();
  broadcast(code, { t: 'bossend', id: b.id, reason: 'dead', type: b.type, last: r.last, rewards });
}
setInterval(() => {                                           // 1초마다: 출현 시각이 됐는지 / 머무는 시간이 끝났는지
  const now = Date.now();
  for (const [code, g] of groups) {
    const cfg = g.boss, r = bossRun.get(code);
    if (r && r.cur && now >= r.cur.until) endBoss(code, g, 'time');
    if (!cfg || !cfg.on) continue; const rr = runOf(code);
    if (!rr.cur && now >= rr.nextAt) { if (online(code)) spawnBoss(code, g); else rr.nextAt = now + cfg.everyMin * 60000; }   // 아무도 접속 안 했으면 이번 차례는 건너뜀
  }
}, 1000);

// ---- 랜덤퀴즈 ----
// 방장이 ON/OFF 와 주기(분)를 정하면, 주기마다 "퀴즈 시작" 초대가 뜬다. 1분 안에 수락/거절 -> 전원이 답하거나 1분이 지나면 시작.
// 수락한 사람만 참여. 주제 1개(시계/게임/애니)를 무작위로 골라 그 주제에서 QUIZ_N 문제. 정답을 맞힌 사람은 모두 경험치. (정답은 서버만 알고, 문제가 나갈 때는 보기만 보냄)
const QUIZ_BANK = [{"t":"clock","q":"다음 중 일본에서 탄생한 시계 브랜드는?","c":["롤렉스","오메가","세이코","브라이틀링"],"a":2},{"t":"clock","q":"일반적인 쿼츠 시계가 시간을 측정하는 데 사용하는 것은?","c":["수정 진동자","태엽의 장력","진자의 왕복","원자 전이 주파수"],"a":0},{"t":"clock","q":"롤렉스의 대표적인 다이버 워치 컬렉션은?","c":["데이토나","익스플로러","데이트저스트","서브마리너"],"a":3},{"t":"clock","q":"오토매틱 기계식 시계가 손목의 움직임으로 태엽을 감는 데 주로 사용하는 부품은?","c":["크라운","로터","베젤","밸런스 휠"],"a":1},{"t":"clock","q":"크로노그래프의 대표적인 기능은?","c":["다른 시간대 표시","경과시간 측정","월별 날짜 보정","달의 위상 표시"],"a":1},{"t":"clock","q":"기계식 시계에서 파워리저브가 의미하는 것은?","c":["최대 방수 깊이","하루 허용 오차","완전히 감았을 때 작동 가능한 시간","밸런스의 진동수"],"a":2},{"t":"clock","q":"1969년 출시된 세이코 쿼츠 아스트론 35SQ가 유명한 이유는?","c":["최초의 상용 쿼츠 손목시계","최초의 오토매틱 손목시계","최초의 LCD 손목시계","최초의 GPS 손목시계"],"a":0},{"t":"clock","q":"GMT 기능의 주요 목적은?","c":["심박수 측정","잠수 시간 계산","달의 위상 확인","다른 시간대의 시각 표시"],"a":3},{"t":"clock","q":"다음 중 파텍 필립의 대표적인 컬렉션은?","c":["로열 오크","노틸러스","리베르소","산토스"],"a":1},{"t":"clock","q":"투르비용은 원래 어떤 문제를 줄이기 위해 고안되었는가?","c":["부식","자기장","중력에 따른 자세 오차","충격에 의한 유리 파손"],"a":2},{"t":"clock","q":"시계 사양에서 '28,800 vph'가 뜻하는 것은?","c":["시간당 진동 횟수","분당 회전수","하루 오차(초)","시간당 와인딩 횟수"],"a":0},{"t":"clock","q":"다음 중 시계의 컴플리케이션(기본 시각 표시 외의 부가 기능)에 해당하지 않는 것은?","c":["퍼페추얼 캘린더","미닛 리피터","사파이어 크리스털","문페이즈"],"a":2},{"t":"clock","q":"오메가 스피드마스터 프로페셔널과 가장 깊이 연관된 역사적 사건은?","c":["에베레스트 최초 등정","남극점 최초 도달","최초의 대서양 무착륙 횡단 비행","아폴로 11호 달 착륙"],"a":3},{"t":"clock","q":"다이버 워치의 단방향 회전 베젤이 주로 한 방향으로만 움직이도록 설계되는 안전상의 이유는?","c":["배터리 소모를 줄이기 위해","실수로 조작해도 경과시간이 실제보다 짧게 표시되는 것을 방지하기 위해","방수 성능을 높이기 위해","시침의 오차를 줄이기 위해"],"a":1},{"t":"clock","q":"스위스 크로노미터 인증기관 COSC가 주로 검증하는 것은?","c":["시계의 중고 거래 가치","무브먼트의 정확도","금속 소재의 순도","디자인의 독창성"],"a":1},{"t":"clock","q":"현대 시계의 일반적인 스위스 레버 이스케이프먼트에서 탈진기와 밸런스의 움직임을 연결하는 부품은?","c":["팔레트 포크","로터","날짜 디스크","배럴"],"a":0},{"t":"clock","q":"조지 다니엘스가 개발한 코액시얼 이스케이프먼트의 핵심 설계 목표는?","c":["날짜 표시의 자동화","탈진기 접촉면의 미끄럼 마찰 감소","태엽 없는 기계식 시계 구현","기계식 시계의 디지털 표시"],"a":1},{"t":"clock","q":"1972년 출시된 오데마 피게 로열 오크의 오리지널 디자인을 담당한 인물은?","c":["아브라함 루이 브레게","제랄드 젠타","한스 빌스도르프","조지 다니엘스"],"a":1},{"t":"clock","q":"28,800 vph인 기계식 무브먼트의 진동 주파수는 일반적인 Hz 기준으로 얼마인가? (1회 진동 = 반진동 2회)","c":["2Hz","3Hz","4Hz","5Hz"],"a":2},{"t":"clock","q":"퍼페추얼 캘린더가 일반적인 애뉴얼 캘린더와 다른 중요한 특징은? (그레고리력 기준의 일반적인 구현)","c":["주말을 자동 표시한다","윤년의 2월 날짜까지 기계적으로 반영한다","시간대 변경을 자동 인식한다","크로노그래프가 반드시 포함된다"],"a":1},{"t":"game","q":"《프래그마타(PRAGMATA)》를 개발한 게임 회사는?","c":["스퀘어 에닉스","캡콤","반다이 남코","코지마 프로덕션"],"a":1},{"t":"game","q":"《레드 데드 리뎀션 2》 본편의 주인공 이름은?","c":["존 마스턴","더치 반 더 린드","아서 모건","잭 마스턴"],"a":2},{"t":"game","q":"《더 라스트 오브 어스》 1편에서 조엘이 미국을 횡단하며 호송하는 소녀는?","c":["애비","엘리","디나","사라"],"a":1},{"t":"game","q":"《디아블로 IV》 본편에서 주요 적대자로 등장하는 악마는?","c":["안다리엘","디아블로","메피스토","릴리트"],"a":3},{"t":"game","q":"《갓 오브 워》(2018)에서 크레토스의 아들 이름은?","c":["발두르","아트레우스","티르","마그니"],"a":1},{"t":"game","q":"《프래그마타》에서 우주복을 입은 주인공 휴와 함께 행동하는 안드로이드 소녀의 이름은?","c":["에이다","다이애나","알로이","이브"],"a":1},{"t":"game","q":"《레드 데드 리뎀션 2》 본편의 주요 시대적 배경은?","c":["1865년","1880년","1899년","1911년"],"a":2},{"t":"game","q":"《더 라스트 오브 어스》 세계관에서 인류를 위협하는 감염 사태의 원인은?","c":["바이러스","기생 곰팡이","방사능","외계 미생물"],"a":1},{"t":"game","q":"《디아블로》 시리즈에서 천사와 악마의 전쟁이 벌어지는 가운데 인간들이 살아가는 세계의 이름은?","c":["아제로스","성역(생츄어리)","트리스트럼","판다리아"],"a":1},{"t":"game","q":"《엘든 링》에서 데미갓들이 차지해 서로 전쟁을 벌인 엘든 링의 파편은?","c":["위대한 룬","왕의 소울","고룡석","데미갓의 인장"],"a":0},{"t":"game","q":"《레드 데드 리뎀션 2》에서 아서 모건이 속한 갱단의 지도자는?","c":["마이카 벨","더치 반 더 린드","호제아 매튜스","콜름 오드리스콜"],"a":1},{"t":"game","q":"《바이오하자드 4》에서 레온이 구출하러 가는 인물은?","c":["질 발렌타인","클레어 레드필드","애슐리 그레이엄","레베카 체임버스"],"a":2},{"t":"game","q":"《사이버펑크 2077》에서 주인공 V의 머릿속에 디지털 인격으로 존재하는 인물은?","c":["아담 스매셔","재키 웰스","조니 실버핸드","타케무라 고로"],"a":2},{"t":"game","q":"《디아블로 II》에서 룬워드를 정상적으로 제작하기 위한 조건은?","c":["유니크 아이템에 룬을 장착한다","정해진 룬을 적합한 홈 아이템에 순서대로 장착한다","룬을 무작위로 조합한다","모든 홈에 같은 룬을 장착한다"],"a":1},{"t":"game","q":"《더 위쳐 3: 와일드 헌트》에서 게롤트가 찾아다니는, 특별한 혈통과 능력을 가진 인물은?","c":["트리스","예니퍼","시리","프리실라"],"a":2},{"t":"game","q":"《프래그마타》의 핵심 게임플레이 특징으로 가장 적절한 것은?","c":["턴제 카드 배틀","실시간 사격과 해킹 퍼즐의 결합","대규모 실시간 전략 전투","리듬 액션 기반 전투"],"a":1},{"t":"game","q":"《다크 소울 1》의 아노르 론도에서 등장하는 유명한 2인 보스 조합은?","c":["온슈타인과 스모우","시프와 아르토리우스","니토와 그윈","가고일과 쿠라그"],"a":0},{"t":"game","q":"《데스 스트랜딩》에서 샘 포터 브리지스가 주로 수행하는 활동은?","c":["현상금 사냥","물자 배송과 거점 연결","우주 기지 건설","군대 지휘"],"a":1},{"t":"game","q":"《레드 데드 리뎀션》 1편에서 존 마스턴이 추적하도록 강요받는 과거 갱단 동료 중 한 명은?","c":["레니 서머스","아서 모건","빌 윌리엄슨","션 맥과이어"],"a":2},{"t":"game","q":"《디아블로 IV》 세계관에서 릴리트와 함께 성역의 창조에 관여한 천사는?","c":["티리엘","이나리우스","임페리우스","말티엘"],"a":1},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 나츠키 스바루가 가진 특별한 능력은?","c":["미래 예지","사망귀환","시간 정지","기억 조작"],"a":1},{"t":"anime","q":"《전생했더니 슬라임이었던 건에 대하여》에서 리무루 템페스트가 전생한 종족은?","c":["드래곤","고블린","슬라임","마족"],"a":2},{"t":"anime","q":"《장송의 프리렌》에서 프리렌과 함께 마왕을 토벌한 용사의 이름은?","c":["힘멜","슈타르크","하이터","자인"],"a":0},{"t":"anime","q":"《오버로드》에서 아인즈 울 고운이 원래 플레이하던 가상현실 MMORPG의 이름은?","c":["아인크라드","위그드라실","엘더 테일","뉴 월드"],"a":1},{"t":"anime","q":"《무직전생》에서 주인공 루데우스 그레이랫이 어린 시절 마법을 배운 스승은?","c":["실피에트","에리스","록시","키시리카"],"a":2},{"t":"anime","q":"《소드 아트 온라인》의 첫 번째 게임에서 플레이어들이 갇힌 거대 부유성의 이름은?","c":["알브헤임","언더월드","아인크라드","건 게일"],"a":2},{"t":"anime","q":"《이 멋진 세계에 축복을!》에서 카즈마와 함께 이세계로 내려온 여신은?","c":["에리스","아쿠아","크리스","위즈"],"a":1},{"t":"anime","q":"《귀멸의 칼날》에서 최초로 등장하는 상현 3의 혈귀 이름은?","c":["도우마","코쿠시보","아카자","규타로"],"a":2},{"t":"anime","q":"《주술회전》에서 후시구로 메구미가 사용하는 대표적인 술식은?","c":["십종영법술","무하한 주술","적혈조술","투사주법"],"a":0},{"t":"anime","q":"《던전밥》에서 라이오스 일행이 던전을 탐험하며 마물을 먹는 가장 큰 이유는?","c":["전설의 요리책을 완성하기 위해","식량비를 아끼면서 파린을 구출하기 위해","던전의 왕이 되기 위해","마물 요리 대회에 참가하기 위해"],"a":1},{"t":"anime","q":"《메이드 인 어비스》에서 심계 6층에서 5층으로 상승할 때 일반적으로 나타나는 상승 부하의 대표적인 결과는?","c":["시력 저하","심한 두통","인간성 상실 또는 죽음","일시적인 기억 상실"],"a":2},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 렘과 람의 종족은?","c":["엘프","수인","오니","드워프"],"a":2},{"t":"anime","q":"《전생했더니 슬라임이었던 건에 대하여》에서 리무루가 처음 만나는 폭풍룡의 이름은?","c":["벨자드","벨그린드","베루도라(벨드라)","밀림"],"a":2},{"t":"anime","q":"《장송의 프리렌》에서 프리렌의 스승이었던 전설적인 마법사는?","c":["제리에","플람메","페른","위벨"],"a":1},{"t":"anime","q":"《노 게임 노 라이프》에서 소라와 시로가 소환된 세계의 이름은?","c":["디스보드","아인크라드","루그니카","나자릭"],"a":0},{"t":"anime","q":"《오버로드》에서 알베도가 소속된 나자릭 지하대분묘에서 맡고 있는 직책은?","c":["제1계층 수호자","수호자 총괄","전투 메이드 대장","정보부 총책임자"],"a":1},{"t":"anime","q":"《무직전생》에서 대규모 전이 재해가 발생한 사건을 통상 무엇이라 부르는가?","c":["피트아령 전이 사건","라플라스 전쟁","인마대전","아슬라 왕국 붕괴 사건"],"a":0},{"t":"anime","q":"《진격의 거인》에서 시조의 거인의 힘을 온전히 활용하는 데 중요한 특별한 혈통은?","c":["아커만 일족","동양인 일족","왕가의 혈통","전사 후보생 혈통"],"a":2},{"t":"anime","q":"《슈타인즈 게이트》에서 오카베 린타로가 자칭하는 매드 사이언티스트 이름은?","c":["존 타이터","호오인 쿄우마","미스터 브라운","아마데우스"],"a":1},{"t":"anime","q":"《Re:제로부터 시작하는 이세계 생활》에서 탐욕의 마녀로 알려진 인물은?","c":["에키드나","사테라","미네르바","세크메트"],"a":0}];
const QUIZ_TOPICS = { clock: '시계', game: '게임', anime: '애니' };
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
  const r = quizOf(code), now = Date.now(), tops = Object.keys(QUIZ_TOPICS), topic = tops[crypto.randomInt(tops.length)];
  const pool = QUIZ_BANK.map((q, i) => i).filter((i) => QUIZ_BANK[i].t === topic); for (let i = pool.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [pool[i], pool[j]] = [pool[j], pool[i]]; }
  Object.assign(r, { phase: 'invite', id: crypto.randomBytes(4).toString('hex'), topic, order: pool.slice(0, QUIZ_N), i: 0, resp: new Map(), players: new Map(), answers: new Map(), score: new Map(), until: now + QUIZ_INVITE_MS });
  broadcast(code, quizInvite(r, now));
}
const quizInvite = (r, now) => ({ t: 'quiz', id: r.id, topic: r.topic, topicName: QUIZ_TOPICS[r.topic], n: QUIZ_N, until: r.until, now });
function quizProg(code, r) { let yes = 0, no = 0; for (const v of r.resp.values()) v ? yes++ : no++; broadcast(code, { t: 'qprog', id: r.id, yes, no, total: liveUids(code).size }); }
function startQuestions(code, g) {
  const r = quizOf(code), live = liveUids(code);
  for (const [uid, yes] of r.resp) if (yes && live.has(uid)) { const mem = memberOf(g, uid); if (mem) r.players.set(uid, mem.name); }
  if (!r.players.size) return endQuiz(code, g, 'noone');
  broadcast(code, { t: 'qstart', id: r.id, topic: r.topic, topicName: QUIZ_TOPICS[r.topic], n: QUIZ_N, players: [...r.players].map(([uid, name]) => ({ uid, name })) });
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
  return { code, name: g.name, owner: g.owner === uid, max: MAX, xp: (g.xp || {})[uid] || 0,
    quiz: { ...quizCfg(g), active: !!(quizRun.get(code) && quizRun.get(code).phase !== 'idle'), nextInMs: quizCfg(g).on ? Math.max(0, ((quizRun.get(code) || {}).nextAt || 0) - now) : null },
    boss: { ...cfg, active: r && r.cur ? { type: r.cur.type, leftMs: Math.max(0, r.cur.until - now) } : null, nextInMs: cfg.on && !(r && r.cur) ? Math.max(0, ((r && r.nextAt) || 0) - now) : null },
    members: g.members.map((m) => ({ uid: m.uid, name: m.name, look: m.look, owner: m.uid === g.owner, me: m.uid === uid, online: live.has(m.uid), state: live.get(m.uid) || 'offline' })) };
}

const ops = {
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
    spawnBoss(code, g); return {};
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
      const run = bossRun.get(code), now = Date.now();
      send(ws, { t: 'welcome', id: ws.id, peers, offline, xps: g.xp, boss: bossPublic(run && run.cur, now), bosscfg: bossCfg(g) });
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
    } else if (m.t === 'bthrow') {                                      // 보스에게 던지기: 모두에게 같이 보이게 전달 (피해는 bhit 로 따로)
      const run = bossRun.get(ws.room), now = Date.now(); if (!run || !run.cur || now >= run.cur.until || now - ws.lastBthrow < 150) return; ws.lastBthrow = now;
      broadcast(ws.room, { t: 'bthrow', from: ws.id, item: String(m.item || '').slice(0, 16) });
    } else if (m.t === 'bhit') {                                        // 공격이 보스에게 맞음: 맞춘 쪽 앱이 알려 줌. 피해 1~10 으로 제한, 연속 요청은 제한
      const run = bossRun.get(ws.room), now = Date.now(), g = groups.get(ws.room);
      if (!run || !run.cur || !g || run.cur.id !== String(m.id) || now >= run.cur.until || run.cur.hp <= 0 || now - ws.lastBhit < 120) return; ws.lastBhit = now;
      const dmg = Math.min(run.cur.hp, Math.min(20, Math.max(1, Math.floor(Number(m.dmg) || 1))));
      run.cur.hp -= dmg; run.dmg.set(ws.uid, (run.dmg.get(ws.uid) || 0) + dmg); run.last = ws.uid;
      broadcast(ws.room, { t: 'bhp', id: run.cur.id, hp: run.cur.hp, by: ws.id, dmg });
      if (run.cur.hp <= 0) endBoss(ws.room, g, 'dead');
    } else if (m.t === 'qjoin') {                                      // 퀴즈 참여 수락/거절 (초대 중에만). 지금 접속한 사람이 모두 정하면 바로 시작
      const r = quizRun.get(ws.room), g = groups.get(ws.room); if (!r || !g || r.phase !== 'invite' || r.id !== String(m.id)) return;
      r.resp.set(ws.uid, !!m.yes); quizProg(ws.room, r);
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
    const g = groups.get(ws.room), member = !!(g && memberOf(g, ws.uid));
    if (!r.size) rooms.delete(ws.room); else broadcast(ws.room, { t: 'leave', id: ws.id, uid: ws.uid, member });
  });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 30000);
server.listen(PORT, '0.0.0.0', () => console.log(`pixel-pals server listening on :${PORT} (그룹 ${groups.size}개 로드, 채팅 ${RETAIN_MS / 3600000}시간 보관)`));
