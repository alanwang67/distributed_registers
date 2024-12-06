package server

import (
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/charmbracelet/log"
)

// Server represents a server in the ABD protocol
type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	mutex   sync.RWMutex // Use RWMutex for better performance with concurrent reads
	Version uint64       // Version number for the server
	Value   uint64       // Value stored in the server
}

type ReadRequest struct{}

type ReadReply struct {
	Version uint64
	Value   uint64
}

type WriteRequest struct {
	Version uint64
	Value   uint64
}

type WriteReply struct{}

// Creates a new server with the given ID, connection, and list of peers.
func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	return &Server{
		Id:      id,
		Self:    self,
		Peers:   peers,
		Version: 0,
		Value:   0,
	}
}

// Handles a read request and replies with the current version and value.
func (s *Server) HandleReadRequest(req *ReadRequest, reply *ReadReply) error {
	s.mutex.RLock() // Use RLock for read operations
	defer s.mutex.RUnlock()

	reply.Version = s.Version
	reply.Value = s.Value

	log.Debugf("Server %d: Handled ReadRequest, Version=%d, Value=%d", s.Id, s.Version, s.Value)
	return nil
}

// Handles a write request and updates the server's version and value if the
// request's version is greater than the server's version.
func (s *Server) HandleWriteRequest(req *WriteRequest, reply *WriteReply) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if req.Version > s.Version {
		s.Version = req.Version
		s.Value = req.Value
		log.Debugf("Server %d: Updated Version=%d, Value=%d", s.Id, s.Version, s.Value)
	}
	return nil
}

// Starts the server and listens for incoming connections.
func (s *Server) Start() error {
	log.Debugf("Starting server %d", s.Id)

	// Configure the listener with SO_REUSEADDR/SO_REUSEPORT
	tcpAddr, err := net.ResolveTCPAddr(s.Self.Network, s.Self.Address)
	if err != nil {
		log.Fatalf("Server %d: Failed to resolve address: %v", s.Id, err)
		return err
	}

	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		if opErr, ok := err.(*net.OpError); ok {
			log.Errorf("Socket reuse error for server %d: %v", s.Id, opErr)
		}
		return err
	}
	defer l.Close()
	log.Infof("Server %d listening on %s", s.Id, s.Self.Address)

	// Register the RPC server
	if err := rpc.RegisterName("Server", s); err != nil {
		log.Fatalf("Server %d: Failed to register RPC: %v", s.Id, err)
		return err
	}

	// Graceful shutdown handling
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-shutdownCh
		log.Infof("Server %d shutting down", s.Id)
		l.Close()
		os.Exit(0)
	}()

	// Accept incoming connections
	for {
		conn, err := l.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				log.Errorf("Server %d: Listener closed", s.Id)
				break
			}
			log.Errorf("Server %d accept error: %v", s.Id, err)
			continue
		}

		// Set a connection timeout
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second) // Example: 30-second keep-alive timeout
			tcpConn.SetDeadline(time.Now().Add(30 * time.Second))
		}

		log.Infof("Server %d: Accepted connection from %s", s.Id, conn.RemoteAddr())
		go func(c net.Conn) {
			defer c.Close()
			rpc.ServeConn(c)
		}(conn)
	}

	return nil
}
