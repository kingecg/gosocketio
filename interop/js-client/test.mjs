import { io } from "socket.io-client";

const port = process.argv[2];
const url = `http://127.0.0.1:${port}`;

let failures = 0;
function check(name, cond, extra = "") {
  if (cond) {
    console.log(`ok - ${name}`);
  } else {
    failures++;
    console.error(`FAIL - ${name}${extra ? `: ${extra}` : ""}`);
  }
}

const connect = (ns = "/", opts = {}) =>
  new Promise((resolve, reject) => {
    const s = io(url + ns, { transports: ["polling", "websocket"], ...opts });
    s.on("connect", () => resolve(s));
    s.on("connect_error", (err) => reject(err));
  });

// ---- 1. default namespace, text event with ack ----
const s = await connect();
check("connect default namespace", s.connected);

const echoAck = await new Promise((resolve) => {
  s.emit("echo", "hello", (reply) => resolve(reply));
});
check("text event with ack", echoAck === "ack:hello", String(echoAck));

// ---- 2. binary event with binary ack ----
const binAck = await new Promise((resolve) => {
  s.emit("binget", Buffer.from("binary-data"), (reply) => resolve(reply));
});
check(
  "binary event with ack",
  Buffer.isBuffer(binAck) && binAck.toString() === "pong:binary-data",
  `type=${typeof binAck} value=${String(binAck)}`
);

// ---- 3. binary pushed by the server ----
const pushed = new Promise((resolve) => s.once("binpush", (b) => resolve(b)));
await new Promise((resolve) => s.emit("binpush?", resolve));
const pushedBuf = await pushed;
check(
  "server push binary",
  Buffer.isBuffer(pushedBuf) && pushedBuf.toString() === "hi-binary",
  `type=${typeof pushedBuf} value=${String(pushedBuf)}`
);

// ---- 4. server emits with ack, client replies with binary ----
const gotAckFromServer = new Promise((resolve) => {
  s.once("needbinack", (data, cb) => {
    cb(Buffer.from("client-binary"));
    resolve(data);
  });
});
const binackResult = new Promise((resolve) => {
  s.emit("needbinack?", (ok) => resolve(ok));
});
const needData = await gotAckFromServer;
const needOk = await binackResult;
check("server binary ack received client binary", needOk === "ok:client-binary", String(needOk));
check("server pushed binary payload before ack", needData === "trigger", String(needData));

// ---- 5. custom namespace ----
const admin = await connect("/admin");
check("connect custom namespace", admin.connected);
const adminAck = await new Promise((resolve) => {
  admin.emit("adminping", (reply) => resolve(reply));
});
check("custom namespace event", adminAck === "admin:pong", String(adminAck));

// ---- 6. unknown namespace rejected ----
let rejected = false;
try {
  const bad = await connect("/nonexistent", { reconnection: false, timeout: 3000 });
  bad.close();
} catch {
  rejected = true;
}
check("unknown namespace rejected", rejected);

// ---- 7. server-initiated disconnect reason ----
const reason = await new Promise((resolve) => {
  s.on("disconnect", resolve);
  s.emit("kick");
});
check("server disconnect reason", reason === "io server disconnect", String(reason));

s.close();
admin.close();

if (failures === 0) {
  console.log("ALL PASS");
} else {
  console.error(`${failures} FAILURES`);
  process.exit(1);
}
