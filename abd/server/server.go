package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// ServerConfig represents the configuration of a peer server.
type ServerConfig struct {
	ID      int    `json:"id"`
	Network string `json:"network"`
	Address string `json:"address"`
}

// Server represents a single server in the distributed system.
type Server struct {
	ID      int
	Address string
	Value   int
	Version int
	Peers   []*ServerConfig // Peer servers
	mu      sync.Mutex
}

// NewServer creates a new server instance.
func NewServer(id int, address string, peers []*ServerConfig) *Server {
	return &Server{
		ID:      id,
		Address: address,
		Peers:   peers,
	}
}

// Start initializes the server and listens for incoming client connections.
func (s *Server) Start() error {
	// Start periodic logging
	go s.periodicLog()

	// Start server listener
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		log.Fatalf("Server %d failed to start: %v", s.ID, err)
		return err
	}
	log.Printf("Server %d listening on %s", s.ID, s.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// handleConnection handles incoming client requests.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	var request map[string]interface{}
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&request)
	if err != nil {
		log.Println("Error decoding request:", err)
		return
	}

	log.Printf("Server %d received request: %v", s.ID, request)

	response := make(map[string]interface{})
	switch request["type"] {
	case "read":
		// Handle read request
		s.mu.Lock()
		response["value"] = s.Value
		response["version"] = s.Version
		s.mu.Unlock()
		log.Printf("Server %d handled read: value=%d, version=%d", s.ID, s.Value, s.Version)
	case "write":
		// Handle write request
		value, okValue := request["value"].(float64)
		version, okVersion := request["version"].(float64)
		if !okValue || !okVersion {
			response["error"] = "Invalid write request"
			log.Printf("Server %d received invalid write request: %v", s.ID, request)
			break
		}
		s.mu.Lock()
		if int(version) > s.Version {
			s.Value = int(value)
			s.Version = int(version) // Use the provided version from the client
			log.Printf("Server %d updated state: value=%d, version=%d", s.ID, s.Value, s.Version)
		} else {
			log.Printf("Server %d ignored write with outdated version: %d", s.ID, int(version))
		}
		s.mu.Unlock()
		response["status"] = "ok"
	default:
		response["error"] = "Unknown operation"
		log.Printf("Server %d received unknown operation: %v", s.ID, request)
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		log.Println("Error encoding response:", err)
	}
}

// periodicLog periodically logs server state and peer connections.
func (s *Server) periodicLog() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		s.logState()
	}
}

// logState logs the server's current state and peer connections.
func (s *Server) logState() {
	s.mu.Lock()
	defer s.mu.Unlock()

	peerInfo := make([]string, len(s.Peers))
	for i, peer := range s.Peers {
		peerInfo[i] = fmt.Sprintf("Peer ID: %d, Address: %s", peer.ID, peer.Address)
	}

	log.Printf("[Server %d] Current State:", s.ID)
	log.Printf("    Value: %d, Version: %d", s.Value, s.Version)
	log.Printf("    Peers: %v", peerInfo)
}

// Enhanced logging for broadcasting
func (s *Server) logClientBroadcast(clientID, serverID int, requestType string) {
	log.Printf("[Client %d] Broadcasting %s request to Server %d", clientID, requestType, serverID)
}

// Enhanced logging for reading
func (s *Server) logClientRead(clientID, serverID int) {
	log.Printf("[Client %d] Attempting to read from Server %d", clientID, serverID)
}
