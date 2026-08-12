import { Server } from "socket.io";
import http from "node:http";

// Official socket.io server driven by the gosocketio Go client. It listens on
// an ephemeral port and prints "PORT <n>" so the Go test can discover it.
const httpServer = http.createServer();
const io = new Server(httpServer, {
  cors: { origin: "*" },
});

io.on("connection", (socket) => {
  socket.on("echo", (data, cb) => {
    cb("ack:" + data);
  });
  socket.on("binget", (data, cb) => {
    cb(Buffer.from("pong:" + data.toString()));
  });
  socket.on("binpush?", () => {
    socket.emit("binpush", Buffer.from("hi-binary"));
  });
  socket.on("needbinack?", (cb) => {
    socket.emit("needbinack", "trigger", (bin) => {
      cb("ok:" + bin.toString());
    });
  });
  socket.on("kick", () => {
    socket.disconnect();
  });
});

io.of("/admin").on("connection", (socket) => {
  socket.on("adminping", (cb) => {
    cb("admin:pong");
  });
});

httpServer.listen(0, () => {
  const { port } = httpServer.address();
  console.log(`PORT ${port}`);
});
