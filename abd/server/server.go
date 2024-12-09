package server

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
)

type Server struct {
	ID      uint64                 // Unique ID of the server
	Address string                 // Server address
	Peers   []*protocol.Connection // List of peer servers
	mutex   sync.RWMutex           // Protects access to the register state
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

// getConnection retrieves or establishes a connection to a peer (used by heartbeat).
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
	log.Printf("Server %d: Processing READ - (Version: %d, Value: %d)", s.ID, s.Version, s.Value)
	return nil
}

// HandleReadConfirm processes a read confirmation from the client.
func (s *Server) HandleReadConfirm(req *protocol.ReadConfirmRequest, reply *protocol.ReadConfirmReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Printf("Server %d: Processing READCONFIRM - Incoming Version: %d, Current (V:%d,Val:%d)",
		s.ID, req.Version, s.Version, s.Value)

	if req.Version > s.Version {
		s.Version = req.Version
		s.Value = req.Value
		log.Printf("Server %d: READCONFIRM Update -> (V:%d,Val:%d)",
			s.ID, s.Version, s.Value)
	} else {
		log.Printf("Server %d: READCONFIRM Ignored - No Update", s.ID)
	}

	reply.Acknowledged = true
	return nil
}

// HandleWriteRequest processes a write request from the client and updates the register if the version is newer.
func (s *Server) HandleWriteRequest(req *protocol.WriteRequest, reply *protocol.WriteReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Printf("Server %d: Processing WRITE - Incoming Version: %d, Current (V:%d,Val:%d)",
		s.ID, req.Version, s.Version, s.Value)

	if req.Version >= s.Version {
		s.Version = req.Version
		s.Value = req.Value
		go s.PropagateWrite(req.Version, req.Value)
		log.Printf("Server %d: WRITE Update -> (V:%d,Val:%d)",
			s.ID, s.Version, s.Value)
	} else {
		log.Printf("Server %d: WRITE Ignored - Incoming Version < Current Version", s.ID)
	}
	return nil
}

// PropagateWrite sends write updates to all peer servers, aggregating results.
func (s *Server) PropagateWrite(version uint64, value uint64) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	failures := 0

	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer *protocol.Connection) {
			defer wg.Done()
			writeReq := protocol.WriteRequest{Version: version, Value: value}
			var reply protocol.WriteReply
			err := protocol.Invoke(*peer, "Server.HandleWriteRequest", &writeReq, &reply)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures++
			} else {
				successes++
			}
		}(peer)
	}

	wg.Wait()
	log.Printf("Server %d: WRITE Propagation Complete - (V:%d,Val:%d), Successes: %d, Failures: %d",
		s.ID, version, value, successes, failures)
}

// SendHeartbeat periodically contacts peers to verify liveness and update the Alive map.
func (s *Server) SendHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mutex.RLock()
		version := s.Version
		value := s.Value
		s.mutex.RUnlock()

		var wg sync.WaitGroup
		for _, peer := range s.Peers {
			wg.Add(1)
			go func(peer *protocol.Connection) {
				defer wg.Done()
				client, err := s.getConnection(peer)
				if err != nil {
					s.mutex.Lock()
					s.Alive[peer.Address] = false
					s.mutex.Unlock()
					return
				}
				heartbeat := protocol.Heartbeat{Version: version, Value: value}
				var reply protocol.HeartbeatReply
				err = client.Call("Server.ReceiveHeartbeat", heartbeat, &reply)
				s.mutex.Lock()
				if err != nil {
					s.Alive[peer.Address] = false
				} else {
					s.Alive[peer.Address] = true
				}
				s.mutex.Unlock()
			}(peer)
		}
		wg.Wait()
	}
}

// ReceiveHeartbeat processes heartbeat messages from peers (no logging for simplicity).
func (s *Server) ReceiveHeartbeat(req *protocol.Heartbeat, reply *protocol.HeartbeatReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if req.Version > s.Version {
		s.Version = req.Version
		s.Value = req.Value
	}
	reply.Acknowledged = true
	return nil
}

// Ping allows peers or clients to test connectivity.
func (s *Server) Ping(_ *struct{}, _ *struct{}) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	log.Printf("Server %d: Processing PING - (V:%d,Val:%d)", s.ID, s.Version, s.Value)
	return nil
}

// Start launches the server and begins listening for connections.
func (s *Server) Start() error {
	log.Printf("Starting server %d at %s", s.ID, s.Address)

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

	// Indicate that the server is ready
	s.mutex.RLock()
	log.Printf("Server %d is ready to receive instructions. Current (Version: %d, Value: %d)",
		s.ID, s.Version, s.Value)
	s.mutex.RUnlock()

	go s.SendHeartbeat()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Server %d: Connection error: %v", s.ID, err)
			continue
		}
		log.Printf("Server %d: New client connected from %s", s.ID, conn.RemoteAddr())
		go func(conn net.Conn) {
			defer func() {
				log.Printf("Server %d: Client disconnected from %s", s.ID, conn.RemoteAddr())
				_ = conn.Close()
			}()
			rpc.ServeConn(conn)
		}(conn)
	}
}
