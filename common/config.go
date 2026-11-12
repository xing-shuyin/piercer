package common

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pion/ice/v2"
	"github.com/pion/stun"
	"github.com/pion/webrtc/v3"
)

// ==================== 环境变量配置（公开仓库安全改造） ====================
//
// 优先级：环境变量 > 默认值
//   PIERCER_PUBLIC_IP   公网服务器 IP（信令 + STUN/TURN）
//   PIERCER_STUN_PORT   STUN 端口，默认 3478
//   PIERCER_TURN_PORT   TURN 端口，默认 3478
//   PIERCER_TURN_USER   TURN 用户名，默认 piercer
//   PIERCER_TURN_PASS   TURN 密码，默认空（必须设置才启用 TURN 中继）
//   PIERCER_SIGNAL_ADDR / SIGNAL_ADDR 信令地址，默认 ws://<PUBLIC_IP>:8080/ws
//
// 本地开发：复制 .env.example 为 .env.local（已在 .gitignore 中忽略），
// 或直接 export 环境变量。

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// 默认值（公开仓库绝不硬编码真实 IP；必须用环境变量 PIERCER_PUBLIC_IP 配置）
const (
	defaultPublicServerIP = ""
	defaultSTUNPort       = "3478"
	defaultTURNPort       = "3478"
	defaultTURNUsername   = "piercer"
)

// 可直接引用的变量（启动时从环境变量初始化一次，保持旧代码 common.PublicServerIP 能用）
var (
	// 公网服务器 IP（信令 + STUN/TURN）
	PublicServerIP = getEnv("PIERCER_PUBLIC_IP", defaultPublicServerIP)

	// STUN/TURN 服务端口
	STUNPort = getEnv("PIERCER_STUN_PORT", defaultSTUNPort)
	TURNPort = getEnv("PIERCER_TURN_PORT", defaultTURNPort)

	// TURN 认证信息（与 coturn /etc/turnserver.conf 中的 user= 行保持一致）
	// 注意：默认密码为空，公开仓库绝不硬编码真实密码。
	TURNUsername   = getEnv("PIERCER_TURN_USER", defaultTURNUsername)
	TURNCredential = os.Getenv("PIERCER_TURN_PASS")
)

// GetPublicServerIP 每次调用都读环境变量（支持运行时覆盖，动态生效）
func GetPublicServerIP() string {
	return getEnv("PIERCER_PUBLIC_IP", defaultPublicServerIP)
}

// GetSTUNPort 获取 STUN 端口
func GetSTUNPort() string {
	return getEnv("PIERCER_STUN_PORT", defaultSTUNPort)
}

// GetTURNPort 获取 TURN 端口
func GetTURNPort() string {
	return getEnv("PIERCER_TURN_PORT", defaultTURNPort)
}

// GetTURNUsername 获取 TURN 用户名
func GetTURNUsername() string {
	return getEnv("PIERCER_TURN_USER", defaultTURNUsername)
}

// GetTURNCredential 获取 TURN 密码（默认空）
func GetTURNCredential() string {
	return os.Getenv("PIERCER_TURN_PASS")
}

// GetSignalAddr 获取信令服务器地址
// 优先级：PIERCER_SIGNAL_ADDR > SIGNAL_ADDR > 按 PUBLIC_IP 自动拼接
// 若均未配置则返回空字符串，调用方需报错提示用户配置环境变量。
func GetSignalAddr() string {
	if v := os.Getenv("PIERCER_SIGNAL_ADDR"); v != "" {
		return v
	}
	if v := os.Getenv("SIGNAL_ADDR"); v != "" {
		return v
	}
	if ip := GetPublicServerIP(); ip != "" {
		return fmt.Sprintf("ws://%s:8080/ws", ip)
	}
	return ""
}

// GetSTUNServers 获取用于端口预测的 STUN 列表（自建 + 公共备用）
// 若未配置 PIERCER_PUBLIC_IP，则只返回公共备用 STUN，保证不带真实 IP 也能编译运行。
func GetSTUNServers() []string {
	backups := []string{
		"stun:stun.cloudflare.com:3478",
		"stun:stun.chat.bilibili.com:3478",
	}
	if ip := GetPublicServerIP(); ip != "" {
		return append([]string{fmt.Sprintf("stun:%s:%s", ip, GetSTUNPort())}, backups...)
	}
	return backups
}

// 端口预测范围（对称NAT场景下，向前/向后扫描的端口数）
const (
	PredictPortBefore = 15 // 向前扫描15个端口
	PredictPortAfter  = 35 // 向后扫描35个端口
)

// 本地端口范围（固定端口区间，提升端口预测准确率）
const (
	LocalPortMin = 50000
	LocalPortMax = 50020
)

// ICE 连接超时时间
const ICETimeout = 20 * time.Second

// ==================== ICE 配置生成 ====================

// GetICEServers 获取 ICE 服务器列表（纯 P2P 模式：只配 STUN，不配 TURN）
// 注意：公网云服务器带宽低（实测 ~0.3MB/s），中继兜底会拖慢整体速度，
// 因此项目策略为【只走 P2P 直连，打不通就不传】。
// 如需启用 TURN 中继兜底，请设置 PIERCER_TURN_PASS，并在下方取消注释 TURN 段。
func GetICEServers() []webrtc.ICEServer {
	servers := []webrtc.ICEServer{}
	// 自建 STUN 服务器（公网机 coturn，仅配置了 PIERCER_PUBLIC_IP 才加）
	if ip := GetPublicServerIP(); ip != "" {
		servers = append(servers, webrtc.ICEServer{
			URLs: []string{fmt.Sprintf("stun:%s:%s", ip, GetSTUNPort())},
		})
	}
	// 备用公共 STUN 服务器（国内可达性优先）
	servers = append(servers, webrtc.ICEServer{
		URLs: []string{
			"stun:stun.cloudflare.com:3478",
			"stun:stun.chat.bilibili.com:3478",
		},
	})

	// 按需启用 TURN 中继（默认关闭：PIERCER_TURN_PASS 为空时不加）：
	// if pass := GetTURNCredential(); pass != "" {
	// 	servers = append(servers, webrtc.ICEServer{
	// 		URLs:       []string{fmt.Sprintf("turn:%s:%s", GetPublicServerIP(), GetTURNPort())},
	// 		Username:   GetTURNUsername(),
	// 		Credential: pass,
	// 	})
	// }

	return servers
}

// CreateSettingEngine 创建增强型 ICE 引擎配置
func CreateSettingEngine() webrtc.SettingEngine {
	settingEngine := webrtc.SettingEngine{}

	// 启用 UDP + TCP 双协议打洞
	settingEngine.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeTCP4,
	})

	// 固定本地 UDP 端口范围（预测准确率更高）
	_ = settingEngine.SetEphemeralUDPPortRange(uint16(LocalPortMin), uint16(LocalPortMax))

	// 加大 SCTP 接收窗口（a_rwnd），提高大文件跨公网吞吐（32MB）
	settingEngine.SetSCTPMaxReceiveBufferSize(32 * 1024 * 1024)

	// 设置 ICE 超时时间（断开/失败/保活）
	settingEngine.SetICETimeouts(ICETimeout/2, ICETimeout, 2*time.Second)

	return settingEngine
}

// CreateSettingEngineWithTCP 创建带 ICE-TCP 打洞能力的引擎（需调用方管理 tcpMux 关闭）
// 返回 settingEngine 与 tcpMux；tcpMux 在程序退出时 Close 即可。
func CreateSettingEngineWithTCP() (webrtc.SettingEngine, io.Closer, error) {
	settingEngine := CreateSettingEngine()

	// 建立 ICE-TCP mux，使本端能收集 TCP host 候选，实现 TCP 穿透
	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", LocalPortMin))
	if err != nil {
		// 端口被占则退回随机端口
		listener, err = net.Listen("tcp4", ":0")
		if err != nil {
			return settingEngine, nil, err
		}
	}
	tcpMux := ice.NewTCPMuxDefault(ice.TCPMuxParams{
		Listener: listener,
	})
	settingEngine.SetICETCPMux(tcpMux)

	return settingEngine, tcpMux, nil
}

// ==================== STUN 探测与端口预测 ====================

// STUNResult STUN 探测结果
type STUNResult struct {
	PublicIP   string // 探测到的公网IP
	PublicPort int    // 探测到的公网端口
}

// DiscoverPublicAddress 向单个 STUN 服务器探测公网地址
func DiscoverPublicAddress(stunServer string) (*STUNResult, error) {
	u, err := stun.ParseURI(stunServer)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("udp", u.Host, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client, err := stun.NewClient(conn)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var xorAddr stun.XORMappedAddress
	err = client.Do(stun.MustBuild(stun.TransactionID, stun.BindingRequest),
		func(res stun.Event) {
			if res.Error != nil {
				return
			}
			_ = xorAddr.GetFrom(res.Message)
		})
	if err != nil || xorAddr.IP == nil {
		return nil, fmt.Errorf("STUN探测失败")
	}

	return &STUNResult{
		PublicIP:   xorAddr.IP.String(),
		PublicPort: xorAddr.Port,
	}, nil
}

// ConcurrentSTUNDiscover 并发向多个 STUN 服务器探测
func ConcurrentSTUNDiscover(stunServers []string) []STUNResult {
	var (
		results []STUNResult
		wg      sync.WaitGroup
		mu      sync.Mutex
	)

	for _, srv := range stunServers {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			res, err := DiscoverPublicAddress(s)
			if err != nil {
				return
			}
			mu.Lock()
			results = append(results, *res)
			mu.Unlock()
		}(srv)
	}

	wg.Wait()
	return results
}

// GeneratePredictedCandidates 生成对称NAT端口预测候选列表
// 基于探测到的基础端口，线性生成预测端口，让对端向这些端口打洞
func GeneratePredictedCandidates(publicIP string, basePort int) []webrtc.ICECandidateInit {
	var candidates []webrtc.ICECandidateInit

	start := basePort - PredictPortBefore
	end := basePort + PredictPortAfter
	if start < 1 {
		start = 1
	}
	if end > 65535 {
		end = 65535
	}

	for port := start; port <= end; port++ {
		candidateStr := fmt.Sprintf(
			"candidate:1 1 udp 2130706431 %s %d typ srflx",
			publicIP, port,
		)
		candidates = append(candidates, webrtc.ICECandidateInit{
			Candidate: candidateStr,
		})
	}

	return candidates
}
