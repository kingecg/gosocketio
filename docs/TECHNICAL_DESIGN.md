# gosocketio 技术方案设计

> 需求：使用 Go 实现 socket.io 协议库，包含客户端和服务端。

## 1. 概述

### 1.1 目标

实现一个纯 Go 编写的 socket.io 协议库，提供：

- **服务端（server）**：接入 Socket.IO 客户端（JS / Python / Java 等官方及第三方客户端），支持命名空间、房间、事件、ACK、二进制消息、中间件鉴权、广播、断线重连（自动重连由客户端负责）。
- **客户端（client）**：连接任意符合规范的 Socket.IO 服务端，支持命名空间、事件、ACK、二进制消息、自动重连、退避重试。

### 1.2 设计原则

| 原则 | 说明 |
| --- | --- |
| 协议优先 | 严格遵循 Engine.IO v4 + Socket.IO v5 协议规范，保证与官方客户端/服务端互通 |
| 分层解耦 | Engine.IO（传输层）与 Socket.IO（消息层）完全分离，可独立使用 |
| 并发安全 | 基于 goroutine + channel，提供并发安全的连接管理 |
| 易用 API | 镜像官方 socket.io 的 API 习惯，降低迁移成本 |
| 最小依赖 | 仅依赖一个 WebSocket 库（`nhooyr.io/websocket`），其余全部标准库实现 |

### 1.3 协议分层

Socket.IO 协议由两层组成：

```
┌──────────────────────────────────────┐
│ Socket.IO 协议层 (v5)                 │  命名空间 / 事件 / ACK / 二进制
├──────────────────────────────────────┤
│ Engine.IO 协议层 (v4)                 │  握手 / 心跳 / 传输升级 / 二进制
├──────────────────────────────────────┤
│ 传输层  (WebSocket / HTTP 长轮询)      │
└──────────────────────────────────────┘
```

## 2. 协议细节梳理

### 2.1 Engine.IO 协议（v4）

**握手流程**：客户端先发起 HTTP 长轮询请求，服务端返回 `open` 包完成握手，之后可通过 WebSocket 升级。

```
GET /socket.io/?EIO=4&transport=polling
→ 0{"sid":"xxxx","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}
```

**包类型**：

| 类型码 | 名称 | 说明 |
| --- | --- | --- |
| 0 | open | 握手响应 |
| 1 | close | 关闭 |
| 2 | ping | 心跳请求（服务端发） |
| 3 | pong | 心跳响应（客户端回） |
| 4 | message | 数据消息（封装 Socket.IO 包） |
| 5 | upgrade | 升级 WebSocket |
| 6 | noop | 空包（用于升级期间挂起轮询） |

**轮询传输帧格式**：`<length>:<payload>`，多包用逗号连接，如 `96:0{...},6:4hello`。

**WebSocket 传输**：每条消息一个文本/二进制帧，无长度前缀。

**心跳**：服务端按 `pingInterval` 发送 ping，在 `pingInterval + pingTimeout` 内未收到 pong 则判定断线。

**升级流程**：
1. 客户端在轮询连接上发送 `5` 升级包，同时建立 WebSocket；
2. 服务端通过已建立但挂起的轮询通道返回 `noop` 包（避免旧轮询提前超时）；
3. WebSocket 建立后通过 `upgrade` 确认，之后切换传输。

### 2.2 Socket.IO 协议（v5）

**包类型**：

| 类型码 | 名称 | 说明 |
| --- | --- | --- |
| 0 | CONNECT | 连接命名空间 |
| 1 | DISCONNECT | 断开命名空间 |
| 2 | EVENT | 普通事件 |
| 3 | ACK | 事件确认 |
| 4 | CONNECT_ERROR | 连接被拒绝 |
| 5 | BINARY_EVENT | 含二进制事件 |
| 6 | BINARY_ACK | 含二进制确认 |

**消息编码规则**：`<type>[<namespace>,]<数据>`，根命名空间 `/` 省略。

```
连接根命名空间:       0{"sid":"..."}
连接 admin 命名空间:  0/admin,{"sid":"..."}
事件:               2["chat","hello"]
命名空间事件:        2/admin,["chat","hello"]
ACK 响应:           3[42,"ok"]
CONNECT_ERROR:      4{"message":"认证失败"}
```

**二进制消息（Binary）**：Socket.IO v5 的二进制包用 `{"_placeholder":true,"num":N}` 占位符标记，占位符出现在消息体数据中，原始二进制数据追加在事件数据数组末尾（经 Engine.IO 传输层以二进制帧传输）。

```
51-["user",{"_placeholder":true,"num":0},...] + [binary data...]
```

**ACK 机制**：事件携带回调时，发送方为事件分配单调递增的 `id`，接收方处理完通过 `3[id, result]`（或 `6` BINARY_ACK）回执。

## 3. 总体架构

### 3.1 目录结构

```
gosocketio/
├── go.mod                        # module github.com/kingecg/gosocketio
├── engineio/                     # Engine.IO 传输层（独立可复用）
│   ├── packet.go                 # 包类型定义与编解码
│   ├── payload.go                # 轮询帧长度前缀编解码
│   ├── session.go                # 会话状态机（sid/心跳/传输切换）
│   ├── server.go                 # Engine.IO 服务端
│   ├── server_conn.go            # 服务端连接抽象
│   ├── client.go                 # Engine.IO 客户端
│   ├── client_conn.go            # 客户端连接抽象
│   └── transport/
│       ├── transport.go          # 传输接口定义
│       ├── polling.go            # HTTP 长轮询传输
│       └── websocket.go          # WebSocket 传输
├── socketio/                     # Socket.IO 消息层
│   ├── server.go                 # Socket.IO 服务端
│   ├── client.go                 # Socket.IO 客户端
│   ├── namespace.go              # 命名空间管理
│   ├── room.go                   # 房间管理
│   ├── socket.go                 # 单个 Socket 连接封装
│   ├── packet.go                 # Socket.IO 包类型与编解码
│   ├── parser.go                 # 事件负载解析（含 ACK id、命名空间）
│   ├── ack.go                    # ACK 回调表
│   ├── emitter.go                # 事件分发器（反射/类型化）
│   ├── adapter.go                # 广播适配器接口（支持后续 Redis 扩展）
│   ├── middleware.go             # 连接中间件
│   └── options.go                # Server/Client 配置
├── examples/
│   ├── server/main.go            # 服务端示例
│   ├── client/main.go            # 客户端示例
│   └── chat/                     # 聊天室端到端示例
├── docs/
│   └── TECHNICAL_DESIGN.md       # 本文档
└── README.md
```

### 3.2 分层调用关系

```
Socket.IO Client ◄──Socket.IO 协议──► Socket.IO Server
        │                                    │
        ▼                                    ▼
Engine.IO Client ◄──Engine.IO 协议──► Engine.IO Server
        │                                    │
        ▼                                    ▼
  WebSocket/轮询传输 ◄────── TCP/HTTP ──────► WebSocket/轮询传输
```

### 3.3 依赖选择

- **WebSocket 实现**：`nhooyr.io/websocket`（现为 `github.com/coder/websocket`），纯 Go、context 友好、API 简洁。
- 其余全部基于 `net/http` 与标准库实现，不引入 socket.io 相关第三方库。
- 若需多实例广播，预留 `socketio.Adapter` 接口，后续可提供 Redis Adapter（`github.com/redis/go-redis/v9`）。

## 4. Engine.IO 服务端设计

### 4.1 核心类型

```go
// engineio/server.go
type Server struct {
    opts    *Options              // MaxPayload、PingInterval、PingTimeout、允许的跨域等
    httpSrv *http.Server
    mu      sync.RWMutex
    sessions map[string]*session   // sid → 会话
}

type session struct {
    sid        string
    conn       *serverConn
    upgradeCh  chan struct{}       // 升级完成信号
}

type serverConn struct {
    mu         sync.Mutex
    transport  transport.Transport  // 当前生效的传输
    polling    transport.Transport  // 升级前的轮询传输（升级时用 noop 挂起）
    wsUpgrade  bool                 // 是否已切换到 WebSocket
    sendCh     chan []byte          // 出站包队列
    closeOnce  sync.Once
}

// 传输抽象
type transport.Transport interface {
    ServeHTTP(w http.ResponseWriter, r *http.Request) // 轮询传输用
    Read(ctx context.Context) (packet.Type, []byte, error)
    Write(ctx context.Context, t packet.Type, b []byte) error
    Close() error
}
```

### 4.2 连接生命周期

```
客户端 GET /socket.io/?EIO=4&transport=polling
  → Server.HandleHTTP：校验 EIO 版本、transport 参数
  → 生成 sid，创建 session，返回 open 包
  → 启动心跳 goroutine（按 pingInterval 发 ping，监听 pong）

轮询 GET（读）     → 阻塞等待 sendCh 有包，或超时返回空 payload
轮询 POST（写）     → 解析 payload，按类型处理（pong/message/upgrade）
WebSocket 升级请求  → 校验 upgrade 包 → 建立 WS → 返回 noop 挂起旧轮询
  → session 切换 transport 到 WebSocket → 旧轮询连接关闭
```

### 4.3 心跳实现

```go
func (c *serverConn) heartbeat(ctx context.Context, interval, timeout time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            c.writePacket(ctx, packet.Ping, nil)   // 发 ping
            // 等待 pong，超过 timeout 未收到则关闭
        case <-ctx.Done():
            return
        }
    }
}
```

- 使用 `time.AfterFunc` 或带截止时间的 `context` 监听 pong。
- 断线检测到后清理 session、通知 Socket.IO 层触发 disconnect 事件。

### 4.4 升级与并发写

- **升级**：WebSocket 握手前，先把客户端发来的 `upgrade` 包放入旧轮询发送队列，服务端向旧轮询返回 `noop`，防止旧轮询因长时间无响应被中断；WS 建立成功后切换发送通道。
- **并发写安全**：所有出站包统一写入 `sendCh`，由单一 writer goroutine 串行写出，避免多 goroutine 并发写连接。

## 5. Socket.IO 服务端设计

### 5.1 核心类型

```go
// socketio/server.go
type Server struct {
    engine       *engineio.Server
    namespaces   map[string]*namespace
    config       *ServerConfig
    serveMux     *http.ServeMux   // 暴露 /socket.io/ 端点
}

// socketio/socket.go
type Socket struct {
    id         string
    nsp        *namespace
    conn       *engineio.Conn   // 底层 Engine.IO 连接
    ackTable   *ackTable        // 待 ACK 的请求
    listeners  *emitter
    connected  atomic.Bool
    sendCh     chan []byte
}

// socketio/namespace.go
type namespace struct {
    name      string
    server    *Server
    sockets   map[string]*Socket
    rooms     map[string]map[string]struct{} // room → 成员 socket id
    middlewares []Middleware
    handlers  map[string]func(*Socket, ...any)
}
```

### 5.2 连接接入流程

```
客户端 CONNECT 请求（Engine.IO message 包，内容 0{...} 或 0/nsp,{...}）
  → Socket.IO parser 解码包
  → 定位/创建 namespace
  → 顺序执行 namespace 中间件（鉴权），任一失败 → 发 CONNECT_ERROR 包
  → 成功 → 分配 socket.id，加入 namespace，注册 socket 到 rooms（自动加入自己的房间）
  → 回 CONNECT 确认包 0{"sid":"..."}
  → 触发 server.OnConnect 与 namespace 的 connection 事件
```

### 5.3 事件分发与 ACK

**事件分发**（`emitter.go`）：

- 用户通过 `On(event, handler)` 注册处理器，handler 签名支持：
  - `func(s *Socket, data string)`
  - `func(s *Socket, data map[string]any)` 或自定义 struct（JSON 反序列化）
  - `func(s *Socket) ... any`（返回值作为 ACK 结果）
- 使用反射解析 handler 参数类型，将事件 payload 的 JSON 反序列化为目标类型；解析失败时调用 `OnError`。

**ACK 表**（`ack.go`）：

```go
type ackTable struct {
    mu   sync.Mutex
    seq  uint64                 // 下一个 ACK id
    cb   map[uint64]func([]any) // id → 回调
}
```

- 发送带回调的事件：分配 `seq`，注册回调，`2[id, "event", args...]`。
- 收到 `3[id, result...]` 时查找回调并执行，随后删除。
- 收到带 id 的 `2` 事件：处理完成后自动回 `3[id, result]`。

### 5.4 广播与房间

```go
// socketio/room.go
func (n *namespace) To(room string, event string, args ...any) error
func (n *namespace) ToExcept(room string, except []string, event string, args ...any) error
func (n *namespace) Emit(event string, args ...any) error      // 广播给命名空间全部

// 服务端入口
server.BroadcastToRoom("/", "room1", "chat message", payload)
server.BroadcastToNamespace("/", "news", payload)
```

- 广播通过遍历 namespace 的房间成员表实现。
- **多实例扩展**：房间与在线状态统一走 `adapter.Adapter` 接口。单机默认用内存实现；接入 Redis 后房间表与成员表改为 Redis Hash/Set，广播用 Redis Pub/Sub 分发，实现水平扩展。

### 5.5 二进制消息

- 事件参数中包含 `[]byte` / `byte[]` / `io.Reader` 时，编码为 BINARY_EVENT：
  - 生成 BINARY_EVENT 包，事件数组中二进制占位替换为 `{"_placeholder":true,"num":i}`；
  - 原始二进制块通过 Engine.IO 二进制帧顺序发送；
- 接收端：解析到 BINARY_EVENT 时，按 `num` 将后续二进制块回填到占位位置，再交给事件分发。

### 5.6 连接断开处理

```
收到 DISCONNECT 包 / Engine.IO close / 心跳超时
  → 触发该 socket 的 disconnect 事件（含 "client namespace disconnect" 等 reason）
  → 从 namespace、所有房间移除
  → 清理 ACK 表，标记连接关闭
```

## 6. Socket.IO 客户端设计

### 6.1 核心类型

```go
// socketio/client.go
type Client struct {
    engine      *engineio.Client
    namespaces  map[string]*nspConn
    config      *ClientConfig     // 地址、超时、重连参数、Transport 选择
    reconnects  atomic.Int64
    ackTable    *ackTable
}

// 命名空间连接
type nspConn struct {
    name      string
    socket    *Socket              // 复用与服务端一致的 Socket 抽象
    connected bool
}
```

### 6.2 连接与自动重连

```
Dial(url) → engineio.Dial（先轮询握手，再升级 WebSocket）
  → 自动 CONNECT 根命名空间（0{...}）
  → Connect(ctx, "/admin", auth) 连接额外命名空间

OnDisconnect → 触发重连策略：
  指数退避随机抖动重试：base=1s, max=5min
  maxReconnectAttempts=∞ / 可配置
  重连成功后自动重发因断线失败的 ACK（可选）
```

- 断开后保留已注册事件与命名空间列表，重连后依次重新 CONNECT。
- 提供 `Connect(ctx, path)`、`Namespace(nsp)`、`OnEvent/On/OnError/OnDisconnect/OnConnect` API。

### 6.3 客户端 API 示例

```go
client, err := socketio.Dial(ctx, "https://example.com", &socketio.ClientConfig{
    Reconnect: true,
    PingInterval: 30 * time.Second,
})

client.On("chat message", func(s *socketio.Socket, msg string) {
    log.Println("收到:", msg)
})

client.OnConnect("/", func(s *socketio.Socket) {
    // 带 ACK 的事件
    s.EmitWithAck("login", func(result map[string]any) {
        log.Println("登录结果:", result)
    }, map[string]any{"user": "alice", "pass": "xxx"})
})

client.Namespace("/admin").Connect(ctx, map[string]any{"token": "..."})
```

## 7. 编解码器设计

### 7.1 Engine.IO 包编解码

```go
// engineio/packet.go
type Type int
const (
    Open   Type = 0
    Close  Type = 1
    Ping   Type = 2
    Pong   Type = 3
    Message Type = 4
    Upgrade Type = 5
    Noop   Type = 6
)

func Encode(t Type, data []byte) []byte { return append([]byte{byte(t + '0')}, data...) }
func Decode(b []byte) (Type, []byte, error)
```

### 7.2 轮询 payload 编解码

```go
// engineio/payload.go
// 编码：多个 "length:data" 逗号连接
func EncodePayload(pkts [][]byte) []byte
// 解码：按长度前缀切分（保留可恢复性：长度超过 maxPayload 时从末尾丢弃截断包）
func DecodePayload(b []byte) ([][]byte, error)
```

### 7.3 Socket.IO 包解析

```go
// socketio/parser.go
type PacketType byte
const (
    Connect        PacketType = '0'
    Disconnect     PacketType = '1'
    Event          PacketType = '2'
    Ack            PacketType = '3'
    ConnectError   PacketType = '4'
    BinaryEvent    PacketType = '5'
    BinaryAck      PacketType = '6'
)

type Packet struct {
    Type      PacketType
    Namespace string    // 默认 "/"
    ID        int       // ACK id（0 表示无）
    Data      []byte    // JSON 编码的事件数组
}

// 支持两种负载格式（兼容性与性能）：
//   文本负载： 解析为 JSON 数组
//   二进制负载：parser 提取占位符，与后续二进制块配对回填
```

- parser 采用**单遍扫描**：先读类型码，再按 `,` 分隔命名空间与 JSON 数组，最后从 JSON 数组中提取 ACK id（数组首元素为数字时），避免不必要的二次解析。
- JSON 编解码使用标准库 `encoding/json`，`RawMessage` 延迟绑定到用户回调参数类型。

## 8. 并发模型

```
每个连接（serverConn/nspConn）:
    readLoop  goroutine  → 读包 → 分发到事件/ACK/心跳处理
    writeLoop goroutine  → 从 sendCh 取包 → 序列化写入底层传输

Server:
    每个命名空间一把 RWMutex 保护 sockets/rooms 表
    ACK 表由互斥锁保护（并发 Emit/ACK 安全）
```

- **读写分离**：读写各一个 goroutine，避免锁竞争与死锁。
- **关闭顺序**：先停 readLoop → 关闭 sendCh → writeLoop 排空后退出 → 底层连接关闭。
- 广播时对房间成员快照后再逐个发送，避免持锁等待网络 I/O。

## 9. API 一览

### 9.1 服务端

```go
srv := socketio.NewServer(&socketio.ServerConfig{
    Path:        "/socket.io/",
    PingInterval: 25 * time.Second,
    PingTimeout:  20 * time.Second,
    MaxPayload:   1 << 20,
    CORS:         socketio.AllowAll(),      // 或自定义来源校验
})

srv.OnConnect("/", func(s *socketio.Socket) error {
    return nil  // 返回非 nil 则拒绝连接（发 CONNECT_ERROR）
})
srv.OnEvent("/", "chat message", func(s *socketio.Socket, msg string) string {
    s.BroadcastToRoom("room1", "chat message", msg)
    return "ok"                            // 自动 ACK
})
srv.OnDisconnect("/", func(s *socketio.Socket, reason string) {})

// 命名空间中间件鉴权
srv.Use("/admin", func(s *socketio.Socket, auth map[string]any) error { ... })

go srv.Serve()
defer srv.Close()
http.Handle("/socket.io/", srv)
log.Fatal(http.ListenAndServe(":3000", nil))
```

### 9.2 客户端

```go
client, err := socketio.Dial(ctx, "http://localhost:3000", &socketio.ClientConfig{
    Reconnect:      true,
    ReconnectMaxRetries: -1,
})
client.OnConnect("/", func(s *socketio.Socket) {})
client.OnEvent("/", "chat message", func(s *socketio.Socket, msg string) {})
client.OnError("/", func(s *socketio.Socket, err error) {})
client.OnDisconnect("/", func(s *socketio.Socket, reason string) {})
client.Namespace("/admin").Connect(ctx, map[string]any{"token": "token"})
```

### 9.3 Socket 方法

```go
s.Emit(event string, args ...any) error
s.EmitWithAck(event string, callback func(...any), args ...any) error
s.JoinRoom(room string) / s.LeaveRoom(room string)
s.To(room string).Emit(event, args...)          // 房间定向
s.Disconnect(reason string)
```

## 10. 错误处理与日志

- **错误分层**：`engineio.ErrInvalidPacket`、`ErrHeartbeatTimeout`、`ErrNamespaceNotConnected`、`ErrHandlerMismatch`、`ErrPayloadTooLarge` 等哨兵错误，统一错误值便于 `errors.Is` 判断。
- **容错策略**：
  - 解析失败的单包丢弃并 `OnError`，不影响连接存活；
  - 轮询 payload 超过 `maxPayload` 时按协议丢弃最老包并关闭连接（防止内存耗尽）；
  - 心跳超时、传输升级失败均触发关闭并通知上层。
- **日志**：定义 `Logger` 接口（`Debugf/Infof/Warnf/Errorf`），默认输出到 stderr，可注入 `log/slog` 或第三方 logger。

## 11. 测试策略

| 层级 | 内容 |
| --- | --- |
| 单元测试 | packet/payload/parser 编解码、ACK 表、房间管理、心跳状态机 |
| 协议互通测试 | 服务端 ↔ 官方 `socket.io-client`（JS）互连；客户端 ↔ 官方 `socket.io`（Node 服务端）互连 |
| 端到端测试 | 同一库内 Server+Client 通信、二进制往返、多命名空间、ACK、断线重连 |
| 压力测试 | 1000+ 并发连接、心跳稳定性、广播性能基准（`go test -bench`） |
| 传输切换测试 | 轮询→WebSocket 升级、轮询被中断、升级失败回退 |
| 兼容性测试 | 自定义二进制消息、大 payload、特殊字符事件名 |

测试依赖的 JS 互通测试脚本放在 `test/js/`，通过 `go test` 调用 node 完成，或以 Docker 化的 CI 步骤执行。

## 12. 里程碑

| 阶段 | 交付内容 | 验收标准 |
| --- | --- | --- |
| M1 基础框架 | go.mod、目录结构、Engine.IO packet/payload 编解码 | 单测通过 |
| M2 Engine.IO 服务端 | 握手、心跳、轮询传输、WebSocket 升级、升级切换 | 可用 `socket.io-client` JS 完成握手与心跳 |
| M3 Socket.IO 服务端 | CONNECT/事件/ACK/命名空间/房间/中间件/广播 | JS 客户端聊天室可运行 |
| M4 二进制支持 | BINARY_EVENT/BINARY_ACK、占位符回填 | 服务端↔JS 客户端二进制互传通过 |
| M5 Engine.IO/Socket.IO 客户端 | Dial、CONNECT、事件、ACK、命名空间、自动重连 | 可接入官方 Node 服务端 |
| M6 工程化 | 示例、README、CI（单测+互通测试）、基准 | 全量测试通过 |
| M7 扩展（可选） | Redis Adapter、动态命名空间、`OnAny` 事件钩子 | 多实例广播一致 |

## 13. 风险与应对

| 风险 | 应对 |
| --- | --- |
| 协议细节易踩坑（升级时序、payload 截断、二进制占位） | 编写协议级互通测试（对齐官方 JS 实现）作为回归保障 |
| WebSocket 库 API 变化 | 仅依赖 `nhooyr.io/websocket` 稳定子集，封装于 transport 层，便于替换 |
| 高并发广播性能 | 内存实现 + 快照广播 + 可选 Redis 适配；提供基准测试定位瓶颈 |
| 与官方行为差异导致的兼容问题 | 建立 `compat/` 互通测试用例集，逐条对齐 |
