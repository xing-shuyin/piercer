package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
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

const (
	// 共享文件根目录（可用环境变量 SHARED_DIR 覆盖）
	fileRootDir = "./shared"
)

func main() {
	sharedDir := fileRootDir
	if v := os.Getenv("SHARED_DIR"); v != "" {
		sharedDir = v
	}
	// 信令地址优先级：PIERCER_SIGNAL_ADDR > SIGNAL_ADDR > 按 PIERCER_PUBLIC_IP 自动拼接
	signalAddr := common.GetSignalAddr()
	if signalAddr == "" {
		log.Fatal("未配置信令地址：请设置环境变量 PIERCER_PUBLIC_IP 或 PIERCER_SIGNAL_ADDR，例如：PIERCER_PUBLIC_IP=你的公网IP ./piercer-server")
	}

	// 自动创建共享目录
	_ = os.MkdirAll(sharedDir, 0755)

	log.Println("=== Piercer 文件传输服务端 ===")
	log.Printf("共享目录: %s", sharedDir)
	log.Println("等待客户端连接...")

	// 1. 连接信令服务器
	conn, _, err := websocket.DefaultDialer.Dial(signalAddr, nil)
	if err != nil {
		log.Fatal("连接信令服务器失败:", err)
	}
	defer conn.Close()
	log.Println("信令服务器已连接")

	// 增强引擎（单次创建，供所有会话复用）
	settingEngine, tcpMux, err := common.CreateSettingEngineWithTCP()
	if err != nil {
		log.Fatal("创建增强引擎失败:", err)
	}
	defer tcpMux.Close()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// 当前活动的 PeerConnection（每新来一个客户端会重建）
	var pcMu sync.Mutex
	var activePC *webrtc.PeerConnection

	closePC := func() {
		pcMu.Lock()
		if activePC != nil {
			_ = activePC.Close()
			activePC = nil
		}
		pcMu.Unlock()
	}

	// 2. 信令消息循环
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Println("信令连接断开:", err)
			break
		}

		var msg SignalMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "offer":
			log.Println("收到客户端Offer，开始处理...")
			if err := handleOffer(conn, api, sharedDir, &pcMu, &activePC, msg.SDP); err != nil {
				log.Println("处理Offer失败:", err)
			}

		case "candidate":
			candidateJSON, _ := json.Marshal(msg.Candidate)
			var candidate webrtc.ICECandidateInit
			_ = json.Unmarshal(candidateJSON, &candidate)

			pcMu.Lock()
			target := activePC
			pcMu.Unlock()
			if target != nil {
				_ = target.AddICECandidate(candidate)
			}
		}
	}

	closePC()
	select {}
}

// handleOffer 为每位客户端创建一个新的 PeerConnection，完成应答并发送 Answer
func handleOffer(conn *websocket.Conn, api *webrtc.API, sharedDir string, pcMu *sync.Mutex, activePC **webrtc.PeerConnection, offerSDP string) error {
	pcMu.Lock()
	// 上一个会话若仍在，先关闭以让出端口
	if *activePC != nil {
		_ = (*activePC).Close()
	}
	pcMu.Unlock()

	config := webrtc.Configuration{
		ICEServers:   common.GetICEServers(),
		BundlePolicy: webrtc.BundlePolicyMaxBundle,
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return err
	}

	// 是否已建立 P2P 直连（策略：仅直连才传输）
	p2pOK := false

	// Trickle ICE：候选即时发送
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

	// 连接状态监听
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("ICE连接状态: %s", state.String())

		switch state {
		case webrtc.ICEConnectionStateConnected:
			if pair, err := pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair(); err == nil {
				log.Println("======================================")
				log.Println("✅ 连接建立成功！")
				log.Printf("   本地候选类型: %s", pair.Local.Typ)
				log.Printf("   远程候选类型: %s", pair.Remote.Typ)
				log.Printf("   本地地址: %s:%d", pair.Local.Address, pair.Local.Port)
				log.Printf("   远程地址: %s:%d", pair.Remote.Address, pair.Remote.Port)

				if pair.Remote.Typ == webrtc.ICECandidateTypeRelay {
					log.Println("⚠️  当前为中继模式，流量经过公网服务器")
				} else {
					log.Println("🚀 当前为P2P直连模式，全速传输")
					p2pOK = true
				}
				log.Println("======================================")
			}

		case webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			// 会话结束，释放引用以便下个客户端重建
			pcMu.Lock()
			if *activePC == pc {
				*activePC = nil
			}
			pcMu.Unlock()
			_ = pc.Close()
		}
	})

	// 数据通道请求处理（策略：仅P2P直连才传输）
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		log.Printf("收到数据通道: %s", dc.Label())
		warmupDone := false // 每会话只预热一次

		// 发送端 SCTP 统计采样（每 5 秒）：发送方 cwnd/rtt/unack 才是吞吐关键
		go func() {
			lastBytes := uint64(0)
			lastSample := time.Now()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				report := pc.GetStats()
				for _, st := range report {
					if sctp, ok := st.(webrtc.SCTPTransportStats); ok && sctp.SmoothedRoundTripTime > 0 {
						now := time.Now()
						dur := now.Sub(lastSample).Seconds()
						if dur > 0 {
							mbps := float64(sctp.BytesSent-lastBytes) / dur / 1024 / 1024
							log.Printf("[SCTP-SRV] rtt=%.0fms cwnd=%.1fKB rwnd=%.1fMB unack=%d sent=%.2fMB/s",
								sctp.SmoothedRoundTripTime*1000,
							float64(sctp.CongestionWindow)/1024,
							float64(sctp.ReceiverWindow)/1024/1024,
							sctp.UNACKData, mbps)
							lastBytes = sctp.BytesSent
							lastSample = now
						}
					}
				}
			}
		}()

		dc.OnOpen(func() {
			log.Println("数据通道已就绪")
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				return
			}

			var req SignalMessage
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				return
			}

			// 非 P2P 直连（中继）时拒绝传输
			if !p2pOK {
				log.Println("⚠️ 当前非P2P直连（中继），按策略拒绝传输")
				refuse, _ := json.Marshal(SignalMessage{
					Type:  "error",
					Error: "非P2P直连，服务端拒绝传输",
				})
				_ = dc.SendText(string(refuse))
				return
			}

			// 首次请求前先做一次预热：把 SCTP 拥塞窗口抬起来，真实文件才能跑高速
			if !warmupDone {
				warmupDone = true
				warmupBytes := int64(64 * 1024 * 1024) // 64MB 假数据
				wm, _ := json.Marshal(SignalMessage{Type: "warmup", Warmup: warmupBytes})
				_ = dc.SendText(string(wm))
				wbuf := make([]byte, 60*1024)
				remaining := warmupBytes
				for remaining > 0 {
					n := int64(len(wbuf))
					if n > remaining {
						n = remaining
					}
					_ = dc.Send(wbuf[:n])
					remaining -= n
				}
				log.Printf("🔥 预热完成: 发送 %d MB 假数据，拥塞窗口已拉高", warmupBytes/1024/1024)
			}

			switch req.Type {
			case "request":
				// 单文件请求
				log.Printf("收到文件请求: %s，起始偏移: %d", req.FileName, req.Offset)
				go sendFile(dc, sharedDir, req.FileName, req.Offset)

			case "batch_request":
				// 批量文件请求
				log.Printf("收到批量文件请求，共 %d 个文件", len(req.FileList))
				go handleBatchRequest(dc, sharedDir, req.FileList)
			}
		})
	})

	// 记录为当前活动会话
	pcMu.Lock()
	*activePC = pc
	pcMu.Unlock()

	// 启动端口预测优化（核心穿透能力），只在尚未建立连接前补充预测候选
	go func() {
		stunServers := common.GetSTUNServers()
		results := common.ConcurrentSTUNDiscover(stunServers)
		if len(results) > 0 {
			log.Printf("STUN探测成功，基础端口: %d", results[0].PublicPort)
			predicted := common.GeneratePredictedCandidates(results[0].PublicIP, results[0].PublicPort)
			log.Printf("生成 %d 个预测端口候选", len(predicted))
			pcMu.Lock()
			target := *activePC
			pcMu.Unlock()
			if target == pc {
				for _, cand := range predicted {
					_ = pc.AddICECandidate(cand)
				}
			}
		}
	}()

	// 应答 Offer
	err = pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	})
	if err != nil {
		_ = pc.Close()
		return err
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return err
	}

	err = pc.SetLocalDescription(answer)
	if err != nil {
		_ = pc.Close()
		return err
	}

	resp := SignalMessage{
		Type: "answer",
		SDP:  answer.SDP,
	}
	respData, _ := json.Marshal(resp)
	_ = conn.WriteMessage(websocket.TextMessage, respData)
	log.Println("已发送Answer")

	return nil
}

// handleBatchRequest 处理批量文件请求（串行传输）
func handleBatchRequest(dc *webrtc.DataChannel, sharedDir string, fileList []FileRequest) {
	for i, f := range fileList {
		log.Printf("[批量 %d/%d] 开始处理: %s，偏移: %d", i+1, len(fileList), f.FileName, f.Offset)
		_ = sendFile(dc, sharedDir, f.FileName, f.Offset)
	}

	// 全部文件传输完成，发送批量结束标记
	batchEnd, _ := json.Marshal(SignalMessage{Type: "batch_end"})
	_ = dc.SendText(string(batchEnd))
	log.Println("✅ 批量文件全部发送完成")
}

// sendFile 发送单个文件，支持断点续传
func sendFile(dc *webrtc.DataChannel, sharedDir, fileName string, offset int64) error {
	filePath := filepath.Join(sharedDir, filepath.Base(fileName))

	// 检查文件是否存在
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		resp, _ := json.Marshal(SignalMessage{
			Type:  "error",
			Error: "文件不存在: " + fileName,
		})
		_ = dc.SendText(string(resp))
		return err
	}

	// 偏移量合法性校验
	if offset > fileInfo.Size() {
		resp, _ := json.Marshal(SignalMessage{
			Type:  "error",
			Error: "偏移量超出文件大小",
		})
		_ = dc.SendText(string(resp))
		return fmt.Errorf("offset overflow")
	}

	// 打开文件并定位到偏移位置
	file, err := os.Open(filePath)
	if err != nil {
		resp, _ := json.Marshal(SignalMessage{
			Type:  "error",
			Error: "打开文件失败",
		})
		_ = dc.SendText(string(resp))
		return err
	}
	defer file.Close()

	if offset > 0 {
		_, err = file.Seek(offset, 0)
		if err != nil {
			resp, _ := json.Marshal(SignalMessage{
				Type:  "error",
				Error: "文件定位失败",
			})
			_ = dc.SendText(string(resp))
			return err
		}
	}

	// 发送文件元信息
	fileInfoMsg, _ := json.Marshal(SignalMessage{
		Type:     "info",
		FileName: fileName,
		FileSize: fileInfo.Size(),
		Offset:   offset,
	})
	_ = dc.SendText(string(fileInfoMsg))

	totalSend := fileInfo.Size() - offset
	log.Printf("开始发送文件: %s (%.2f MB)，本次传输: %.2f MB",
		fileName,
		float64(fileInfo.Size())/1024/1024,
		float64(totalSend)/1024/1024,
	)

	// 分块传输（60KB 块；必须 < pion 读缓冲 65535，64KB 会阻塞接收端 readLoop）
	buf := make([]byte, 60*1024)
	var sentBytes int64
	lastProgress := 0

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("读取文件错误:", err)
			break
		}

		_ = dc.Send(buf[:n])
		sentBytes += int64(n)

		// 进度显示（每10%输出一次）
		if totalSend > 0 {
			progress := int(float64(sentBytes) / float64(totalSend) * 100)
			if progress >= lastProgress+10 {
				log.Printf("发送进度: %s %d%% (%d/%d 字节)",
					fileName, progress, sentBytes+offset, fileInfo.Size())
				lastProgress = progress
			}
		}
	}

	// 发送单文件结束标记
	endMsg, _ := json.Marshal(SignalMessage{
		Type:     "end",
		FileName: fileName,
	})
	_ = dc.SendText(string(endMsg))

	log.Printf("✅ 文件发送完成: %s", fileName)
	return nil
}
