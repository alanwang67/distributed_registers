package server

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/charmbracelet/log"
)

type Server struct {
	ID      uint64                 // Unique ID of the server
	Address string                 // Server address
	Peers   []*protocol.Connection // List of peer servers
	mutex   sync.RWMutex           // Protects access to the register
	Version uint64                 // Current version of the register
	Value   uint64                 // Current value of the register
	Alive   map[string]bool        // Tracks live status of peers
	pool    map[string]*rpc.Client // Connection pool for heartbeats
}

// NewServer initializes the server with given ID, address, and peers.
func NewServer(id uint64, address string, peers []*protocol.Connection) *Server {
	alive := make(map[string]bool)
	pool := make(map[string]*rpc.Client)
	for _, peer := range peers {
		alive[peer.Address] = true
	}
	return &Server{
		ID:      id,
		Address: address,
		Peers:   peers,
		Version: 0,
		Value:   0,
		Alive:   alive,
		pool:    pool,
	}
}

// getConnection retrieves or establishes a connection to a peer.
func (s *Server) getConnection(peer *protocol.Connection) (*rpc.Client, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if client, exists := s.pool[peer.Address]; exists {
		var reply struct{}
		if err := client.Call("Server.Ping", struct{}{}, &reply); err == nil {
			return client, nil
		}
		client.Close()
		delete(s.pool, peer.Address)
	}

	client, err := rpc.Dial(peer.Network, peer.Address)
	if err != nil {
		return nil, err
	}
	s.pool[peer.Address] = client
	return client, nil
}

// HandleReadRequest processes a read request from the client and returns the register state.
func (s *Server) HandleReadRequest(req *protocol.ReadRequest, reply *protocol.ReadReply) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	reply.Version = s.Version
	reply.Value = s.Value
	log.Infof("Server %d: Client READ - Version: %d, Value: %d", s.ID, s.Version, s.Value)
	return nil
}

// HandleReadConfirm processes a read confirmation from the client.
func (s *Server) HandleReadConfirm(req *protocol.ReadConfirmRequest, reply *protocol.ReadConfirmReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if req.Version > s.Version {
		s.Version = req.Version
		s.Value = req.Value
		log.Infof("Server %d: ReadConfirm - Updated Version: %d, Value: %d", s.ID, req.Version, req.Value)
	} else {
		log.Infof("Server %d: ReadConfirm Ignored - Incoming Version: %d <= Current Version: %d", s.ID, req.Version, s.Version)
	}

	reply.Acknowledged = true
	return nil
}

// HandleWriteRequest processes a write request from the client and updates the register state if the version is newer.
func (s *Server) HandleWriteRequest(req *protocol.WriteRequest, reply *protocol.WriteReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if req.Version >= s.Version {
		s.Version = req.Version
		s.Value = req.Value
		go s.PropagateWrite(req.Version, req.Value)
		log.Infof("Server %d: Client WRITE - Updated Version: %d, Value: %d", s.ID, s.Version, s.Value)
	} else {
		log.Warnf("Server %d: Client WRITE Ignored - Incoming Version: %d < Current Version: %d", s.ID, req.Version, s.Version)
	}
	return nil
}

// PropagateWrite sends write updates to peer servers.
func (s *Server) PropagateWrite(version uint64, value uint64) {
	for _, peer := range s.Peers {
		go func(peer *protocol.Connection) {
			writeReq := protocol.WriteRequest{Version: version, Value: value}
			var reply protocol.WriteReply
			err := protocol.Invoke(*peer, "Server.HandleWriteRequest", &writeReq, &reply)
			if err != nil {
				log.Warnf("Server %d: Write Propagation to %s failed: %v", s.ID, peer.Address, err)
			} else {
				log.Infof("Server %d: Write Propagated to %s - Version: %d, Value: %d", s.ID, peer.Address, version, value)
			}
		}(peer)
	}
}

// SendHeartbeat sends periodic heartbeat messages to all peers and logs the server's state.
func (s *Server) SendHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	timeoutCount := 0 // Tracks the number of heartbeat cycles

	for range ticker.C {
		timeoutCount++

		// Collect the current state and peer statuses
		s.mutex.RLock()
		version := s.Version
		value := s.Value
		aliveStatus := make(map[string]bool, len(s.Alive))
		for addr, status := range s.Alive {
			aliveStatus[addr] = status
		}
		s.mutex.RUnlock()

		// Log current state
		log.Infof("==== Server %d: Heartbeat Timeout %d ====", s.ID, timeoutCount)
		log.Infof("Current State: Version: %d, Value: %d", version, value)

		for addr, status := range aliveStatus {
			if status {
				log.Infof("Peer %s: LIVE", addr)
			} else {
				log.Warnf("Peer %s: DOWN", addr)
			}
		}

		// Send heartbeat to all peers
		for _, peer := range s.Peers {
			go func(peer *protocol.Connection) {
				client, err := s.getConnection(peer)
				if err != nil {
					s.mutex.Lock()
					s.Alive[peer.Address] = false
					s.mutex.Unlock()
					log.Warnf("Heartbeat to %s failed: %v", peer.Address, err)
					return
				}
				heartbeat := protocol.Heartbeat{Version: version, Value: value}
				var reply protocol.HeartbeatReply
				err = client.Call("Server.ReceiveHeartbeat", heartbeat, &reply)
				if err != nil {
					s.mutex.Lock()
					s.Alive[peer.Address] = false
					s.mutex.Unlock()
					log.Warnf("Heartbeat to %s failed: %v", peer.Address, err)
				} else {
					s.mutex.Lock()
					s.Alive[peer.Address] = true
					s.mutex.Unlock()
					log.Infof("Heartbeat to %s successful.", peer.Address)
				}
			}(peer)
		}

		log.Infof("==== End of Heartbeat Timeout %d ====", timeoutCount)
	}
}

// ReceiveHeartbeat processes heartbeat messages from peers.
func (s *Server) ReceiveHeartbeat(req *protocol.Heartbeat, reply *protocol.HeartbeatReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if req.Version > s.Version {
		s.Version = req.Version
		s.Value = req.Value
		log.Infof("Server %d: Heartbeat Update - Version: %d, Value: %d", s.ID, req.Version, req.Value)
	}
	reply.Acknowledged = true
	return nil
}

// Ping allows peers or clients to test connectivity.
func (s *Server) Ping(_ *struct{}, _ *struct{}) error {
	return nil
}

// Start launches the server and begins listening for connections.
func (s *Server) Start() error {
	log.Infof("Starting server %d at %s", s.ID, s.Address)
	tcpAddr, err := net.ResolveTCPAddr("tcp", s.Address)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	if err := rpc.Register(s); err != nil {
		return err
	}

	go s.SendHeartbeat()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("Connection error: %v", err)
			continue
		}
		log.Infof("Server %d: New client connected from %s", s.ID, conn.RemoteAddr())
		go func(conn net.Conn) {
			defer func() {
				log.Infof("Server %d: Client disconnected from %s", s.ID, conn.RemoteAddr())
				_ = conn.Close()
			}()
			rpc.ServeConn(conn)
		}(conn)
	}
}
