// Duck World server
// Run with:  npm install   (first time only)   then   node server.js
//
// One process, three jobs:
//   - TCP port 3000: raw sockets (the Windows projector .exe connects here)
//   - Web port 8080: serves the ./public folder (your website) AND accepts
//                    WebSocket connections (Ruffle in the browser connects here)
//
// Protocol: one JSON message per line (ends with \n).
//
// Client -> server
//   {"t":"join","name":"Quackers"}
//   {"t":"move","x":500,"y":420}        (where the duck is walking TO)
//   {"t":"chat","msg":"hello"}
//
// Server -> client
//   {"t":"welcome","id":1,"players":[{id,name,x,y,tx,ty}, ...]}
//   {"t":"join","id":2,"name":"Bob","x":540,"y":450}
//   {"t":"move","id":2,"x":500,"y":420}
//   {"t":"chat","id":2,"msg":"hello"}
//   {"t":"leave","id":2}

const net = require('net');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const TCP_PORT = Number(process.env.TCP_PORT) || 3000;
const WEB_PORT = Number(process.env.PORT) || 8080;
const PUBLIC_DIR = path.join(__dirname, 'docs');

// keep these the same as the floor numbers in your Flash code
const FLOOR = { left: 0, right: 1080, top: 150, bottom: 720 };
const START = { x: 540, y: 450 };

const MAX_PLAYERS = 40;
const MAX_NAME = 16;
const MAX_CHAT = 100;
const MAX_LINE = 1024;       // longest message we accept
const MIN_MSG_GAP_MS = 40;   // ignore anything faster than this per duck

const TALK_GAP_MS = 500;     // chat / quick chat / emotes: at most one every half second
const QUACK_GAP_MS = 350;

// The page builds its menus from these lists (sent in the welcome message), and
// "quick" / "emote" messages only carry a choice from them, never free text.
const QUICK_CHAT = [
  'Hello!', 'Hi there!', 'Bye!', 'Thank you!',
  'Nice duck!', 'Follow me!', 'Want to be friends?', "Let's go to the pond!",
  'Anyone have bread?', 'That was fun!', 'Sorry!', 'Quack!',
];
const EMOTES = ['😀', '😂', '😍', '😎', '😢', '😡', '👍', '❤️', '🎉', '🍞', '🦆', '💤'];

const players = new Map();   // id -> { id, name, x, y, tx, ty, socket, buf, last, joined }
let nextId = 1;

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

// "socket" below is anything with write(string), destroy() and .destroyed
// (a TCP socket as-is, or a small wrapper around a WebSocket)
function send(socket, obj) {
  if (!socket.destroyed) socket.write(JSON.stringify(obj) + '\n');
}

function broadcast(obj, exceptId) {
  for (const p of players.values()) {
    if (p.id !== exceptId && p.joined) send(p.socket, obj);
  }
}

function cleanText(s, max) {
  if (typeof s !== 'string') return '';
  return s.replace(/[\u0000-\u001f]/g, '').trim().slice(0, max);
}

function handle(p, msg) {
  if (!msg || typeof msg !== 'object') return;

  if (msg.t === 'join') {
    if (p.joined) return;
    p.name = cleanText(msg.name, MAX_NAME) || 'Duck' + p.id;
    p.joined = true;

    const others = [];
    for (const o of players.values()) {
      if (o.joined && o.id !== p.id) {
        others.push({ id: o.id, name: o.name, x: o.x, y: o.y, tx: o.tx, ty: o.ty });
      }
    }
    send(p.socket, {
      t: 'welcome', id: p.id, name: p.name, players: others,
      quick: QUICK_CHAT, emotes: EMOTES,
    });
    broadcast({ t: 'join', id: p.id, name: p.name, x: p.x, y: p.y }, p.id);
    console.log(`#${p.id} joined as "${p.name}" (${others.length} others here)`);
    return;
  }

  if (!p.joined) return; // must join first

  if (msg.t === 'move') {
    const x = Number(msg.x), y = Number(msg.y);
    if (!isFinite(x) || !isFinite(y)) return;
    p.tx = clamp(x, FLOOR.left, FLOOR.right);
    p.ty = clamp(y, FLOOR.top, FLOOR.bottom);
    broadcast({ t: 'move', id: p.id, x: p.tx, y: p.ty }, p.id);
    return;
  }

  // chat, quick chat, emotes and quacks go to everyone, including the sender,
  // so every screen shows the same thing at the same moment
  if (msg.t === 'chat') {
    const text = cleanText(msg.msg, MAX_CHAT);
    if (!text) return;
    if (tooSoon(p, 'lastTalk', TALK_GAP_MS)) return;
    broadcast({ t: 'chat', id: p.id, msg: text });
    return;
  }

  if (msg.t === 'quick') {
    const text = QUICK_CHAT[Number(msg.q)];
    if (typeof text !== 'string') return;
    if (tooSoon(p, 'lastTalk', TALK_GAP_MS)) return;
    broadcast({ t: 'chat', id: p.id, msg: text });
    return;
  }

  if (msg.t === 'emote') {
    if (!EMOTES.includes(msg.e)) return;
    if (tooSoon(p, 'lastTalk', TALK_GAP_MS)) return;
    broadcast({ t: 'emote', id: p.id, e: msg.e });
    return;
  }

  if (msg.t === 'quack') {
    if (tooSoon(p, 'lastQuack', QUACK_GAP_MS)) return;
    broadcast({ t: 'quack', id: p.id });
    return;
  }
}

// true if this duck did the same kind of thing less than `gap` ms ago
function tooSoon(p, key, gap) {
  const now = Date.now();
  if (now - (p[key] || 0) < gap) return true;
  p[key] = now;
  return false;
}

// Shared by TCP and WebSocket: registers a duck and returns the two
// functions the transport should call (data in, connection closed).
function attach(conn) {
  if (players.size >= MAX_PLAYERS) {
    conn.destroy();
    return null;
  }

  const p = {
    id: nextId++, name: '', joined: false,
    x: START.x, y: START.y, tx: START.x, ty: START.y,
    socket: conn, buf: '', last: 0,
  };
  players.set(p.id, p);

  return {
    data(chunk) {
      p.buf += chunk;
      if (p.buf.length > MAX_LINE * 4) { conn.destroy(); return; } // flooding

      let nl;
      while ((nl = p.buf.indexOf('\n')) !== -1) {
        const line = p.buf.slice(0, nl).replace(/\r$/, '');
        p.buf = p.buf.slice(nl + 1);
        if (!line || line.length > MAX_LINE) continue;

        const now = Date.now();
        if (now - p.last < MIN_MSG_GAP_MS) continue; // too fast, drop it
        p.last = now;

        let msg;
        try { msg = JSON.parse(line); } catch (e) { continue; }
        handle(p, msg);
      }
    },
    close() {
      if (!players.has(p.id)) return;
      players.delete(p.id);
      if (p.joined) {
        broadcast({ t: 'leave', id: p.id });
        console.log(`#${p.id} "${p.name}" left`);
      }
    },
  };
}

// ---------- raw TCP (projector .exe) ----------
const tcpServer = net.createServer((socket) => {
  socket.setEncoding('utf8');
  socket.setNoDelay(true);

  const h = attach(socket);
  if (!h) return;
  socket.on('data', h.data);
  socket.on('close', h.close);
  socket.on('error', h.close);
});

tcpServer.listen(TCP_PORT, () => {
  console.log(`TCP (projector) listening on port ${TCP_PORT}`);
});

// ---------- website + WebSocket (Ruffle) ----------
const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.swf': 'application/x-shockwave-flash',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.wav': 'audio/wav',
};

function serveStatic(req, res) {
  let urlPath;
  try {
    urlPath = decodeURIComponent(req.url.split('?')[0]);
  } catch (e) {
    res.writeHead(400);
    return res.end();
  }
  if (urlPath === '/') urlPath = '/index.html';

  const file = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) { // no walking out of the public folder
    res.writeHead(403);
    return res.end('forbidden');
  }

  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404);
      return res.end('not found');
    }
    res.writeHead(200, {
      'Content-Type': TYPES[path.extname(file).toLowerCase()] || 'application/octet-stream',
    });
    res.end(data);
  });
}

const webServer = http.createServer(serveStatic);
const wss = new WebSocketServer({ server: webServer });

wss.on('connection', (ws) => {
  const conn = {
    get destroyed() { return ws.readyState !== 1; },
    write(s) { ws.send(Buffer.from(s, 'utf8')); }, // binary frames, like raw socket bytes
    destroy() { ws.terminate(); },
  };

  const h = attach(conn);
  if (!h) return;
  ws.on('message', (data) => h.data(data.toString('utf8')));
  ws.on('close', h.close);
  ws.on('error', h.close);
});

webServer.listen(WEB_PORT, () => {
  console.log(`Website + WebSocket listening on http://localhost:${WEB_PORT}`);
});
