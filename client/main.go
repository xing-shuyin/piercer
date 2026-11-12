package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"

	"piercer/common"
)

// 信令消息结构
type SignalMessage struct {
	Type      string        `json:"type"`
	SDP       string        `json:"sdp,omitempty"`
	Candidate interface{}   `json:"candidate,omitempty"`
	FileName  string        `json:"fileName,omitempty"`
	FileSize  int64         `json:"fileSize,omitempty"`
	Warmup    int64         `json:"warmup,omitempty"` // 预热字节数（收到后丢弃该量假数据）
	Offset    int64         `json:"offset,omitempty"`
	FileList  []FileRequest `json:"fileList,omitempty"`
	Error     string        `json:"error,omitempty"`
}

// FileRequest 单文件请求项
type FileRequest struct {
	FileName string `json:"fileName"`
	Offset   int64  `json:"offset"`
}

// FileReceiver 当前接收文件的状态
type FileReceiver struct {
	FileName     string
	TotalSize    int64
	ReceivedSize int64
	File         *os.File
	LastProgress int
}

func main() {
	// 命令行参数解析
	fileName := flag.String("file", "", "单文件下载，指定文件名")
	filesStr := flag.String("files", "", "批量下载，多个文件名用逗号分隔，如: a.zip,b.iso")
	saveDir := flag.String("save", "./downloads", "文件保存目录")
	signalingAddr := flag.String("signal", common.GetSignalAddr(), "信令服务器地址（可用 PIERCER_SIGNAL_ADDR/SIGNAL_ADDR 环境变量覆盖）")
	flag.Parse()

	if *signalingAddr == "" {
		log.Fatal("未配置信令地址：请设置环境变量 PIERCER_PUBLIC_IP 或 PIERCER_SIGNAL_ADDR，例如：PIERCER_PUBLIC_IP=你的公网IP ./piercer-client -file a.zip")
	}

	if *fileName == "" && *filesStr == "" {
		log.Fatal("请指定下载文件:\n  单文件: ./client -file test.zip\n  批量: ./client -files a.zip,b.iso")
	}

	// 自动创建保存目录
	_ = os.MkdirAll(*saveDir, 0755)

	log.Println("=== Piercer 文件传输客户端 ===")
	if *fileName != "" {
		log.Printf("下载模式: 单文件 (%s)", *fileName)
	} else {
		log.Printf("下载模式: 批量 (%d 个文件)", len(strings.Split(*filesStr, ",")))
	}
	log.Printf("保存目录: %s", *saveDir)

	// 打洞带概率性：ICE 失败时自动重试（最多 3 次，每次全新连接）
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		log.Printf("--- 第 %d/%d 次连接尝试 ---", attempt, maxAttempts)
		code := runTransfer(*fileName, *filesStr, *saveDir, *signalingAddr)
		if code != retryCode {
			os.Exit(code)
		}
	}
	log.Println("❌ 多次尝试仍无法建立 P2P 直连，请稍后重试")
	os.Exit(2)
}

// retryCode 表示本次打洞失败、可重试
const retryCode = 3

// runTransfer 执行一次完整的连接 + 传输流程，返回进程退出码
func runTransfer(fileName, filesStr, saveDir, signalingAddr string) int {
	// 1. 连接信令服务器
	conn, _, err := websocket.DefaultDialer.Dial(signalingAddr, nil)
	if err != nil {
		log.Fatal("连接信令服务器失败:", err)
	}
	defer conn.Close()
	log.Println("信令服务器已连接")

	// 2. 创建增强型 PeerConnection（UDP + TCP 双协议打洞）
	settingEngine, tcpMux, err := common.CreateSettingEngineWithTCP()
	if err != nil {
		log.Fatal("创建增强引擎失败:", err)
	}
	defer tcpMux.Close()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	config := webrtc.Configuration{
		ICEServers:   common.GetICEServers(),
		BundlePolicy: webrtc.BundlePolicyMaxBundle,
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		log.Fatal("创建PeerConnection失败:", err)
		return 1
	}
	defer pc.Close()

	// 3. 创建数据通道
	dc, err := pc.CreateDataChannel("file-transfer", nil)
	if err != nil {
		log.Fatal("创建数据通道失败:", err)
	}

	// 4. ICE 候选即时发送
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		msg := SignalMessage{
			Type:      "candidate",
			Candidate: candidate.ToJSON(),
		}
		data, _ := json.Marshal(msg)
		_ = conn.WriteMessage(websocket.TextMessage, data)
	})

	// 5. 连接状态监听（策略：仅P2P直连才传输）
	var currentReceiver *FileReceiver
	p2pOK := make(chan struct{}, 1)
	exitCode := make(chan int, 1) // 各回调通过它通知主流程退出码
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("ICE连接状态: %s", state.String())

		switch state {
		case webrtc.ICEConnectionStateConnected:
			isP2P := false
			if pair, err := pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair(); err == nil {
				log.Println("======================================")
				log.Println("✅ 连接建立成功！")
				log.Printf("   本地候选类型: %s", pair.Local.Typ)
				log.Printf("   远程候选类型: %s", pair.Remote.Typ)

				if pair.Remote.Typ == webrtc.ICECandidateTypeRelay {
					log.Println("⚠️  当前为中继模式，流量经过公网服务器")
				} else {
					log.Println("🚀 当前为P2P直连模式，全速传输")
					isP2P = true
				}
				log.Println("======================================")
			}

			if !isP2P {
				log.Println("❌ 策略要求仅P2P直连传输，当前无法直连（只能走中继），放弃传输")
				select {
				case exitCode <- 2:
				default:
				}
				return
			}
			select {
			case p2pOK <- struct{}{}:
			default:
			}

		case webrtc.ICEConnectionStateFailed:
			log.Println("❌ P2P连接失败（打洞未成功），本次尝试失败，将自动重试")
			select {
			case exitCode <- retryCode:
			default:
			}
		}
	})

	// 6. 文件接收处理逻辑
	requested := false
	discarding := int64(0) // 预热数据丢弃计数
	dc.OnOpen(func() {
		log.Println("数据通道已打开，等待确认P2P直连...")
		// 等 P2P 确认（一般连接成功即触发），确认后才发送下载请求（只发一次）
		select {
		case <-p2pOK:
			if !requested {
				requested = true
				sendDownloadRequest(dc, fileName, filesStr, saveDir)
			}
		case <-time.After(10 * time.Second):
			log.Println("❌ P2P直连未确认，本次尝试失败，将自动重试")
			select {
			case exitCode <- retryCode:
			default:
			}
		}
	})

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			// 控制消息处理
			var ctrl SignalMessage
			if err := json.Unmarshal(msg.Data, &ctrl); err != nil {
				return
			}

			switch ctrl.Type {
			case "warmup":
				// 服务端预热：丢弃 ctrl.Warmup 字节的假数据（拉高拥塞窗口）
				discarding = ctrl.Warmup
				log.Printf("🔥 收到预热信号，丢弃 %d MB 假数据", ctrl.Warmup/1024/1024)

			case "info":
				// 收到新文件元信息，创建/打开本地文件
				savePath := filepath.Join(saveDir, filepath.Base(ctrl.FileName))
				var file *os.File
				var err error

				if ctrl.Offset > 0 {
					// 断点续传：追加模式打开
					file, err = os.OpenFile(savePath, os.O_APPEND|os.O_WRONLY, 0644)
					if err != nil {
						log.Fatal("打开续传文件失败:", err)
					}
					// 校验本地文件大小与偏移是否一致
					stat, _ := file.Stat()
					if stat.Size() != ctrl.Offset {
						log.Printf("⚠️  本地文件大小不匹配，将从头下载: %s", ctrl.FileName)
						file.Close()
						file, err = os.Create(savePath)
						ctrl.Offset = 0
					}
				} else {
					// 新文件：创建覆盖
					file, err = os.Create(savePath)
					if err != nil {
						log.Fatal("创建保存文件失败:", err)
					}
				}

				currentReceiver = &FileReceiver{
					FileName:     ctrl.FileName,
					TotalSize:    ctrl.FileSize,
					ReceivedSize: ctrl.Offset,
					File:         file,
				}

				log.Printf("开始接收文件: %s (%.2f MB)，已下载: %.2f MB",
					ctrl.FileName,
					float64(ctrl.FileSize)/1024/1024,
					float64(ctrl.Offset)/1024/1024,
				)

			case "end":
				// 单文件接收完成
				if currentReceiver != nil && currentReceiver.File != nil {
					currentReceiver.File.Close()
					log.Printf("✅ 文件下载完成: %s", currentReceiver.FileName)
					currentReceiver = nil
				}
				if fileName != "" {
					// 单文件模式：完成后正常退出
					select {
					case exitCode <- 0:
					default:
					}
				}

			case "batch_end":
				// 全部批量文件完成
				log.Println("🎉 所有批量文件下载完成！")
				select {
				case exitCode <- 0:
				default:
				}

			case "error":
				log.Printf("❌ 服务端错误: %s", ctrl.Error)
				if currentReceiver == nil {
					select {
					case exitCode <- 1:
					default:
					}
				}
			}
		} else {
			// 预热数据：直接丢弃，不写盘
			if discarding > 0 {
				discarding -= int64(len(msg.Data))
				return
			}

			// 文件数据块写入本地
			if currentReceiver == nil || currentReceiver.File == nil {
				return
			}

			_, err := currentReceiver.File.Write(msg.Data)
			if err != nil {
				log.Println("写入文件错误:", err)
				return
			}

			currentReceiver.ReceivedSize += int64(len(msg.Data))

			// 进度显示
			if currentReceiver.TotalSize > 0 {
				progress := int(float64(currentReceiver.ReceivedSize) / float64(currentReceiver.TotalSize) * 100)
				if progress >= currentReceiver.LastProgress+10 {
					log.Printf("接收进度: %s %d%% (%d/%d 字节)",
						currentReceiver.FileName,
						progress,
						currentReceiver.ReceivedSize,
						currentReceiver.TotalSize,
					)
					currentReceiver.LastProgress = progress
				}
			}
		}
	})

	// 7. 启动端口预测优化
	go func() {
		stunServers := common.GetSTUNServers()
		results := common.ConcurrentSTUNDiscover(stunServers)
		if len(results) > 0 {
			log.Printf("STUN探测成功，基础端口: %d", results[0].PublicPort)
			predicted := common.GeneratePredictedCandidates(results[0].PublicIP, results[0].PublicPort)
			log.Printf("生成 %d 个预测端口候选", len(predicted))
			for _, cand := range predicted {
				_ = pc.AddICECandidate(cand)
			}
		}
	}()

	// 8. 创建并发送 Offer
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		log.Fatal("创建Offer失败:", err)
	}

	err = pc.SetLocalDescription(offer)
	if err != nil {
		log.Fatal("设置本地描述失败:", err)
	}

	offerMsg := SignalMessage{
		Type: "offer",
		SDP:  offer.SDP,
	}
	offerData, _ := json.Marshal(offerMsg)
	_ = conn.WriteMessage(websocket.TextMessage, offerData)
	log.Println("已发送Offer，等待服务端响应...")

	// 9. SCTP 传输统计采样（每 5 秒打印一次，用于诊断吞吐瓶颈）
	go func() {
		lastBytes := uint64(0)
		lastSample := time.Now()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats := getSCTPStats(pc)
			if stats != nil {
				now := time.Now()
				dur := now.Sub(lastSample).Seconds()
				if dur > 0 {
					mbps := float64(stats.BytesReceived-lastBytes) / dur / 1024 / 1024
					log.Printf("[SCTP] rtt=%.0fms cwnd=%.1fKB rwnd=%.1fMB unack=%d recv=%.2fMB/s",
						stats.SmoothedRoundTripTime*1000,
						float64(stats.CongestionWindow)/1024,
						float64(stats.ReceiverWindow)/1024/1024,
						stats.UNACKData, mbps)
					lastBytes = stats.BytesReceived
					lastSample = now
				}
			}
		}
	}()

	// 10. 信令消息循环（独立 goroutine）
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				log.Println("信令连接断开:", err)
				select {
				case exitCode <- retryCode:
				default:
				}
				return
			}

			var msg SignalMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "answer":
				log.Println("收到Answer")
				err = pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDP,
				})
				if err != nil {
					log.Println("设置远程描述失败:", err)
				}

			case "candidate":
				candidateJSON, _ := json.Marshal(msg.Candidate)
				var candidate webrtc.ICECandidateInit
				_ = json.Unmarshal(candidateJSON, &candidate)
				_ = pc.AddICECandidate(candidate)
			}
		}
	}()

	// 等待退出码（0=成功，retryCode=可重试，其他=不可重试错误）
	return <-exitCode
}

// sendDownloadRequest 发送下载请求（自动判断单文件/批量模式）
func sendDownloadRequest(dc *webrtc.DataChannel, fileName, filesStr, saveDir string) {
	if fileName != "" {
		// 单文件模式，自动检测本地偏移
		offset := getLocalFileOffset(saveDir, fileName)
		req, _ := json.Marshal(SignalMessage{
			Type:     "request",
			FileName: fileName,
			Offset:   offset,
		})
		_ = dc.SendText(string(req))
		if offset > 0 {
			log.Printf("检测到本地已下载 %.2f MB，自动断点续传", float64(offset)/1024/1024)
		}
	} else {
		// 批量模式，每个文件独立检测偏移
		fileNames := strings.Split(filesStr, ",")
		var fileList []FileRequest
		for _, fn := range fileNames {
			fn = strings.TrimSpace(fn)
			if fn == "" {
				continue
			}
			offset := getLocalFileOffset(saveDir, fn)
			fileList = append(fileList, FileRequest{
				FileName: fn,
				Offset:   offset,
			})
		}

		req, _ := json.Marshal(SignalMessage{
			Type:     "batch_request",
			FileList: fileList,
		})
		_ = dc.SendText(string(req))
		log.Printf("已发送批量请求，共 %d 个文件", len(fileList))
	}
}

// getLocalFileOffset 获取本地已下载文件的字节偏移量
func getLocalFileOffset(saveDir, fileName string) int64 {
	savePath := filepath.Join(saveDir, filepath.Base(fileName))
	stat, err := os.Stat(savePath)
	if err != nil {
		return 0
	}
	return stat.Size()
}

// getSCTPStats 获取当前 SCTP 传输统计（用于诊断）
func getSCTPStats(pc *webrtc.PeerConnection) *webrtc.SCTPTransportStats {
	report := pc.GetStats()
	for _, st := range report {
		if sctp, ok := st.(webrtc.SCTPTransportStats); ok && sctp.SmoothedRoundTripTime > 0 {
			return &sctp
		}
	}
	return nil
}
