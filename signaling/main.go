package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// WebSocket 升级配置
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// 单房间广播结构，点对点场景直接广播所有消息
type Room struct {
	clients map[*websocket.Conn]bool
	mu      sync.Mutex
}

var room = &Room{
	clients: make(map[*websocket.Conn]bool),
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("连接升级失败:", err)
		return
	}
	defer conn.Close()

	// 客户端加入房间
	room.mu.Lock()
	room.clients[conn] = true
	clientCount := len(room.clients)
	room.mu.Unlock()

	log.Printf("客户端接入，当前在线: %d", clientCount)

	// 消息循环：收到消息后广播给其他所有客户端
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			room.mu.Lock()
			delete(room.clients, conn)
			log.Printf("客户端断开，当前在线: %d", len(room.clients))
			room.mu.Unlock()
			break
		}

		room.mu.Lock()
		for c := range room.clients {
			if c != conn {
				_ = c.WriteMessage(websocket.TextMessage, msg)
			}
		}
		room.mu.Unlock()
	}
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	log.Println("=== Piercer 信令服务器 ===")
	log.Println("监听端口: :8080")
	log.Println("WebSocket地址: ws://0.0.0.0:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
