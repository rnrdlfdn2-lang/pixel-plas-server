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
  return { code, name: g.name, owner: g.owner === uid, max: MAX,
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
    groups.delete(code); msgLog.delete(code); persistMsgs(); persist(); return {};
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
    groups.set(code, { name: gnameOf(m.groupName), owner: seen.has(m.owner) ? m.owner : members[0].uid, members });
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
  ws.id = String(nextId++); ws.room = null; ws.uid = null; ws.alive = true; ws.lastChat = 0; ws.lastPulse = 0; ws.state = 'online';
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
      r.set(ws.id, ws); rooms.set(code, r); ws.room = code; ws.uid = m.uid;
      send(ws, { t: 'welcome', id: ws.id, peers, offline });
      broadcast(code, { t: 'peer', id: ws.id, uid: m.uid, name: mem.name, look: mem.look, state: 'online' }, ws.id);
      return;
    }
    if (!ws.room) return;

    if (m.t === 'presence' && (m.state === 'away' || m.state === 'online')) { ws.state = m.state; broadcast(ws.room, { t: 'presence', id: ws.id, state: m.state }, ws.id); }
    else if (m.t === 'typing') broadcast(ws.room, { t: 'typing', id: ws.id, on: !!m.on }, ws.id);
    else if (m.t === 'pulse') { const now = Date.now(); if (now - ws.lastPulse < 900) return; ws.lastPulse = now; broadcast(ws.room, { t: 'pulse', id: ws.id }, ws.id); }   // 크기 효과: 1초에 한 번
    else if (m.t === 'pos' && typeof m.x === 'number') broadcast(ws.room, { t: 'pos', id: ws.id, x: Math.min(1, Math.max(0, m.x)) }, ws.id);
    else if (m.t === 'chat') {
      const now = Date.now(); if (now - ws.lastChat < 300) return; ws.lastChat = now;
      const text = String(m.text || '').trim().slice(0, 200);
      if (text) {
        broadcast(ws.room, { t: 'chat', id: ws.id, text, bubble: String(m.bubble || '').slice(0, 16) });
        const g = groups.get(ws.room), mem = g && memberOf(g, ws.uid); if (mem) pushMsg(ws.room, ws.uid, mem.name, text);
      }
    } else if (m.t === 'throw' && rooms.get(ws.room).has(String(m.to))) {
      broadcast(ws.room, { t: 'throw', from: ws.id, to: String(m.to), item: String(m.item || '').slice(0, 4) });
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
