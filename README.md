# Piercer — 跨内网 P2P 文件传输工具

基于 Pion WebRTC 的跨内网 P2P 文件传输工具，针对校园网 / 家庭宽带 / CGNAT 场景深度优化：
对称 NAT 端口预测 + UDP/TCP 双协议打洞 + Trickle ICE，P2P 直连失败自动降级 TURN 中继，保证 100% 连通。

## 架构

```
[客户端 client]  ←── WebSocket 信令 ──→  [公网服务器: signaling + coturn(STUN/TURN)]
      │                                        │
      └────── WebRTC 直连（P2P）◄──────  [文件服务端 server]（内网，如实验室）
                    或 TURN 中继兜底
```

- **signaling/**：公网 WebSocket 信令服务器，仅交换 SDP/ICE，不参与业务数据
- **server/**：文件服务端，共享指定目录，响应下载请求（支持断点续传/批量）
- **client/**：文件客户端，主动发起连接，支持单文件/批量/自动断点续传
- **common/**：公共配置、STUN 探测、对称 NAT 端口预测

## 本地编译

```bash
go build -o signaling-server ./signaling
go build -o piercer-server ./server
go build -o piercer-client ./client
```

## 使用

```bash
# 公网机：启动信令
./signaling-server

# 内网机：文件服务端（shared 目录放共享文件）
# 环境变量覆盖默认值（推荐，公开仓库不再硬编码真实 IP/密码）
PIERCER_PUBLIC_IP=你的公网IP SHARED_DIR=/opt/piercer/shared ./piercer-server
# 或显式指定信令地址：
# PIERCER_SIGNAL_ADDR=ws://你的公网IP:8080/ws SHARED_DIR=/opt/piercer/shared ./piercer-server

# 任意机：下载（自动断点续传）
./piercer-client -file lab_data.iso
./piercer-client -files "a.zip,b.iso" -save ./lab_files
# 指定信令（环境变量方式）：
# PIERCER_SIGNAL_ADDR=ws://你的公网IP:8080/ws ./piercer-client -file lab_data.iso
```

## 环境变量（公开仓库安全改造后）

| 变量 | 说明 | 默认值 |
|---|---|---|
| `PIERCER_PUBLIC_IP` | 公网服务器 IP（信令 + STUN/TURN）**必填** | 无默认值，必须设置 |
| `PIERCER_SIGNAL_ADDR` / `SIGNAL_ADDR` | 信令地址（优先级高于 PUBLIC_IP 拼接） | `ws://<PUBLIC_IP>:8080/ws`，未设 IP 则必填 |
| `PIERCER_STUN_PORT` | STUN 端口 | `3478` |
| `PIERCER_TURN_USER` / `PIERCER_TURN_PASS` | TURN 账号密码（默认关闭中继，设密码才启用） | 用户 `piercer` / 密码空 |
| `SHARED_DIR` | 服务端共享目录 | `./shared` |

本地开发复制模板：`cp .env.example .env.local`（`.env.local` 已忽略，不会提交）。

## 部署速查

- 信令：`deploy/piercer-signaling.service` → 公网机 `/etc/systemd/system/`
- 服务端：`deploy/piercer-server.service` → 内网机 `/etc/systemd/system/`
- TURN/STUN：公网机 coturn，`/etc/turnserver.conf` 需 `user=piercer:你的强密码`（模板见 `deploy/turnserver.conf`，真实密码绝不提交）
- 端口：tcp 8080/3478，udp 3478（安全组放行）

## 穿透原理

1. **端口预测**：对称 NAT 端口线性分配，STUN 探测基础端口后生成 ±15/35 预测候选打洞
2. **双协议**：UDP + TCP 同时打洞，UDP 优先，TCP 穿透防火墙更强
3. **Trickle ICE**：候选即时收发，边收集边打洞
4. **断点续传**：客户端携带本地偏移请求，服务端 seek 后续传
5. **中继兜底**：P2P 打不通自动降级 TURN，传输对上层透明
