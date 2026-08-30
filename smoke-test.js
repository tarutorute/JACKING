// One-off local smoke test. Starts the server and exercises:
//   create -> join (seat p2) -> ready
//   relay forwarding between the two players
//   a 3rd client joining as a spectator
//   snapshot caching (spectator gets the last snapshot on join)
//   snapshot fan-out (spectator gets later snapshots)
//   spectator count fan-out, and peer_left on player disconnect
// then exits with code 0 (all passed) or 1 (something failed).
const { spawn } = require("child_process");
const WebSocket = require("ws");

const PORT = 8799;
const server = spawn(process.execPath, ["server.js"], {
  cwd: __dirname,
  env: { ...process.env, PORT: String(PORT) },
  stdio: ["ignore", "pipe", "pipe"],
});
server.stdout.on("data", (d) => process.stdout.write(`[server] ${d}`));
server.stderr.on("data", (d) => process.stderr.write(`[server:err] ${d}`));

let failed = false;
const fail = (m) => { failed = true; console.error("FAIL:", m); };
const ok = (m) => console.log("OK:", m);
const url = `ws://127.0.0.1:${PORT}`;

// Each client keeps a running log of every message it received, so tests can wait
// for a message regardless of whether it arrived before or after the wait started.
function client() {
  const ws = new WebSocket(url);
  ws.inbox = [];
  ws.on("message", (raw) => ws.inbox.push(JSON.parse(raw.toString())));
  ws.ready = new Promise((res) => ws.on("open", res));
  return ws;
}
function waitFor(ws, pred, label) {
  return new Promise((res, rej) => {
    const deadline = Date.now() + 3000;
    const tick = () => {
      const hit = ws.inbox.find(pred);
      if (hit) return res(hit);
      if (Date.now() > deadline) return rej(new Error("timeout waiting for " + (label || "message")));
      setTimeout(tick, 20);
    };
    tick();
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  await sleep(500);

  // --- host creates a room ---
  const host = client();
  await host.ready;
  host.send(JSON.stringify({ t: "create", pass: "hunter2" }));
  const created = await waitFor(host, (m) => m.t === "created", "created");
  const room = created.room;
  if (/^[A-Z0-9]{6}$/.test(room)) ok(`room created: ${room}`);
  else fail(`bad room id: ${room}`);

  // --- guest joins as p2, both get ready ---
  const guest = client();
  await guest.ready;
  guest.send(JSON.stringify({ t: "join", room, pass: "hunter2" }));
  const joined = await waitFor(guest, (m) => m.t === "joined", "joined");
  if (joined.seat === "p2") ok("guest took player seat p2");
  else fail(`guest seat unexpected: ${JSON.stringify(joined)}`);
  await waitFor(host, (m) => m.t === "ready", "host ready");
  await waitFor(guest, (m) => m.t === "ready", "guest ready");
  ok("both players got ready");

  // --- relay between the two players ---
  guest.send(JSON.stringify({ t: "relay", data: { hello: "from guest" } }));
  const relayed = await waitFor(host, (m) => m.t === "relay", "relay");
  if (relayed.data.hello === "from guest") ok("relay host<-guest");
  else fail("relay payload wrong at host");

  // --- host publishes a snapshot, then a spectator joins ---
  host.send(JSON.stringify({ t: "snapshot", data: { phase: "battle", turnCount: 3 }, rev: 1 }));
  await sleep(100);

  const spec = client();
  await spec.ready;
  spec.send(JSON.stringify({ t: "join", room, pass: "hunter2" }));
  const specJoined = await waitFor(spec, (m) => m.t === "joined", "spec joined");
  if (specJoined.seat === "spec") ok("third client joined as spectator");
  else fail(`third client seat unexpected: ${JSON.stringify(specJoined)}`);

  const firstSnap = await waitFor(spec, (m) => m.t === "snapshot", "cached snapshot");
  if (firstSnap.data && firstSnap.data.turnCount === 3) ok("spectator got the cached snapshot on join");
  else fail(`cached snapshot wrong: ${JSON.stringify(firstSnap)}`);

  await waitFor(host, (m) => m.t === "spectators" && m.count === 1, "spectator count = 1");
  ok("players notified: 1 spectator");

  // --- later snapshot fans out to the spectator ---
  guest.send(JSON.stringify({ t: "snapshot", data: { phase: "battle", turnCount: 4 }, rev: 2 }));
  await waitFor(spec, (m) => m.t === "snapshot" && m.data.turnCount === 4, "later snapshot");
  ok("spectator got the later snapshot");

  // --- spectators do not receive raw relay traffic ---
  const specInboxLen = spec.inbox.length;
  host.send(JSON.stringify({ t: "relay", data: { secret: 1 } }));
  await sleep(150);
  if (!spec.inbox.slice(specInboxLen).some((m) => m.t === "relay")) ok("spectator does not see raw relay traffic");
  else fail("spectator leaked a relay message");

  // --- player disconnect notifies the other player and the spectator ---
  guest.close();
  await waitFor(host, (m) => m.t === "peer_left", "host peer_left");
  ok("host got peer_left when guest dropped");
  await waitFor(spec, (m) => m.t === "peer_left", "spec peer_left");
  ok("spectator got peer_left when a player dropped");

  // --- unknown room rejected ---
  const bad = client();
  await bad.ready;
  bad.send(JSON.stringify({ t: "join", room: "ZZZZZZ", pass: "nope" }));
  const err = await waitFor(bad, (m) => m.t === "err", "err");
  if (err.reason === "not_found") ok("unknown room rejected");
  else fail(`expected not_found, got ${JSON.stringify(err)}`);
  bad.close();

  host.close();
  spec.close();
  await sleep(200);
  console.log(failed ? "\n=== SMOKE TEST: FAILED ===" : "\n=== SMOKE TEST: ALL PASSED ===");
  server.kill();
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error(e);
  server.kill();
  process.exit(1);
});
