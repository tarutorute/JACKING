// JACKING relay + web server.
//
// Two jobs, one process:
//   1. Serve the game itself (the static files in ./public) so a single deployed
//      URL is the whole thing - players and spectators just open it in a browser.
//   2. Relay messages inside a password-protected "room". A room has two player
//      seats and any number of spectator connections. The server still knows
//      nothing about the card game: it forwards player moves verbatim and keeps a
//      copy of the latest full-state "snapshot" a player published so that a
//      spectator who shows up mid-game can be handed the current board.
//
// Protocol (client -> server):
//   {t:"create", pass}                 -> creates a room, caller becomes seat 0 (p1)
//   {t:"join", room, pass}             -> joins a room: seat 1 (p2) if free, else spectator
//   {t:"relay", data}                  -> forwarded verbatim to the OTHER player seat
//   {t:"snapshot", data, rev}          -> full game state; cached and pushed to spectators
//
// Protocol (server -> client):
//   {t:"created", room}                -> reply to create (caller is p1)
//   {t:"joined", room, seat}           -> reply to join; seat is "p2" or "spec"
//   {t:"ready"}                        -> sent to BOTH players once a room has 2 of them
//   {t:"relay", data}                  -> a message the other player sent
//   {t:"snapshot", data, rev}          -> current full game state (for spectators)
//   {t:"want_snapshot"}                -> asks a player to publish a fresh snapshot
//   {t:"spectators", count}            -> current spectator count (sent to everyone)
//   {t:"peer_left", who}               -> who: "player" | "spectator"
//   {t:"err", reason}                  -> need_password | not_found | bad_password | room_full

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = path.join(__dirname, "public");
const ROOM_ID_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; // no 0/O/1/I/L to avoid mixups
const ROOM_ID_LENGTH = 6;
const ABANDONED_ROOM_MS = 30 * 60 * 1000; // rooms nobody ever joined get swept after 30 min
const SWEEP_INTERVAL_MS = 5 * 60 * 1000;
const MAX_MEMBERS_PER_ROOM = 64; // 2 players + up to 62 spectators

/**
 * @typedef {Object} Room
 * @property {string} pass
 * @property {[import('ws').WebSocket|null, import('ws').WebSocket|null]} seats  player seats (p1, p2)
 * @property {Set<import('ws').WebSocket>} spectators
 * @property {number} createdAt
 * @property {any} snapshot        latest full-state snapshot published by a player
 * @property {number} snapshotRev  revision number of that snapshot
 */
/** @type {Map<string, Room>} */
const rooms = new Map();

function genRoomId() {
  let id;
  do {
    id = "";
    for (let i = 0; i < ROOM_ID_LENGTH; i++) {
      id += ROOM_ID_CHARS[Math.floor(Math.random() * ROOM_ID_CHARS.length)];
    }
  } while (rooms.has(id));
  return id;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function roomMemberCount(room) {
  let n = room.spectators.size;
  if (room.seats[0]) n++;
  if (room.seats[1]) n++;
  return n;
}

function broadcastSpectatorCount(room) {
  const payload = { t: "spectators", count: room.spectators.size };
  if (room.seats[0]) send(room.seats[0], payload);
  if (room.seats[1]) send(room.seats[1], payload);
  for (const spec of room.spectators) send(spec, payload);
}

function anyPlayer(room) {
  return room.seats[0] || room.seats[1] || null;
}

/* ----------------------------- static file serving ----------------------------- */

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".svg": "image/svg+xml",
  ".json": "application/json; charset=utf-8",
};

function serveStatic(req, res) {
  let urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  if (urlPath === "/" || urlPath === "") urlPath = "/index.html";

  if (urlPath === "/health") {
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    res.end(`JACKING relay OK - ${rooms.size} room(s) open\n`);
    return;
  }

  // resolve inside PUBLIC_DIR and refuse anything that escapes it
  const filePath = path.join(PUBLIC_DIR, path.normalize(urlPath));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end("forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      // unknown path -> hand back the app shell so deep links like /?room=ABC work
      fs.readFile(path.join(PUBLIC_DIR, "index.html"), (err2, shell) => {
        if (err2) {
          res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
          res.end("not found");
        } else {
          res.writeHead(200, { "content-type": MIME[".html"] });
          res.end(shell);
        }
      });
      return;
    }
    res.writeHead(200, { "content-type": MIME[path.extname(filePath)] || "application/octet-stream" });
    res.end(data);
  });
}

const server = http.createServer(serveStatic);

/* --------------------------------- websockets --------------------------------- */

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.roomId = null;
  ws.role = null; // 0 | 1 | "spec"

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!msg || typeof msg.t !== "string") return;

    if (msg.t === "create") {
      const pass = String(msg.pass || "").slice(0, 64);
      if (!pass) {
        send(ws, { t: "err", reason: "need_password" });
        return;
      }
      const roomId = genRoomId();
      rooms.set(roomId, {
        pass,
        seats: [ws, null],
        spectators: new Set(),
        createdAt: Date.now(),
        snapshot: null,
        snapshotRev: -1,
      });
      ws.roomId = roomId;
      ws.role = 0;
      send(ws, { t: "created", room: roomId });
      return;
    }

    if (msg.t === "join") {
      const roomId = String(msg.room || "").toUpperCase();
      const room = rooms.get(roomId);
      if (!room) {
        send(ws, { t: "err", reason: "not_found" });
        return;
      }
      if (room.pass !== String(msg.pass || "")) {
        send(ws, { t: "err", reason: "bad_password" });
        return;
      }
      if (roomMemberCount(room) >= MAX_MEMBERS_PER_ROOM) {
        send(ws, { t: "err", reason: "room_full" });
        return;
      }
      ws.roomId = roomId;

      if (!room.seats[1]) {
        // take the open player seat
        room.seats[1] = ws;
        ws.role = 1;
        send(ws, { t: "joined", room: roomId, seat: "p2" });
        send(room.seats[0], { t: "ready" });
        send(room.seats[1], { t: "ready" });
        broadcastSpectatorCount(room);
        return;
      }

      // both player seats taken -> spectator
      room.spectators.add(ws);
      ws.role = "spec";
      send(ws, { t: "joined", room: roomId, seat: "spec" });
      if (room.snapshot != null) {
        send(ws, { t: "snapshot", data: room.snapshot, rev: room.snapshotRev });
      } else {
        send(anyPlayer(room), { t: "want_snapshot" });
      }
      broadcastSpectatorCount(room);
      return;
    }

    if (msg.t === "relay") {
      if (ws.roomId === null || (ws.role !== 0 && ws.role !== 1)) return;
      const room = rooms.get(ws.roomId);
      if (!room) return;
      const peer = room.seats[ws.role === 0 ? 1 : 0];
      send(peer, { t: "relay", data: msg.data });
      return;
    }

    if (msg.t === "snapshot") {
      if (ws.roomId === null || (ws.role !== 0 && ws.role !== 1)) return;
      const room = rooms.get(ws.roomId);
      if (!room) return;
      const rev = typeof msg.rev === "number" ? msg.rev : room.snapshotRev + 1;
      if (rev < room.snapshotRev) return; // stale
      room.snapshot = msg.data;
      room.snapshotRev = rev;
      for (const spec of room.spectators) {
        send(spec, { t: "snapshot", data: msg.data, rev });
      }
      return;
    }
  });

  ws.on("close", () => {
    if (ws.roomId === null) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.role === "spec") {
      room.spectators.delete(ws);
      broadcastSpectatorCount(room);
    } else if (ws.role === 0 || ws.role === 1) {
      room.seats[ws.role] = null;
      const other = room.seats[ws.role === 0 ? 1 : 0];
      send(other, { t: "peer_left", who: "player" });
      for (const spec of room.spectators) send(spec, { t: "peer_left", who: "player" });
    }

    if (!room.seats[0] && !room.seats[1] && room.spectators.size === 0) {
      rooms.delete(ws.roomId);
    }
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    const empty = !room.seats[0] && !room.seats[1] && room.spectators.size === 0;
    const abandoned = !room.seats[1] && now - room.createdAt > ABANDONED_ROOM_MS;
    if (empty || abandoned) {
      rooms.delete(id);
    }
  }
}, SWEEP_INTERVAL_MS).unref();

server.listen(PORT, () => {
  console.log(`JACKING relay + web server listening on :${PORT}`);
});
