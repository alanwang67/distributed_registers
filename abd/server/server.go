package server

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/charmbracelet/log"
)

// Server represents a server in the ABD protocol

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	mutex   sync.Mutex
	Version uint64 // Version number for the server
	Value   uint64 // Value stored in the server
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	reply.Version = s.Version
	reply.Value = s.Value

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
	}
	return nil
}

// Starts the server and listens for incoming connections.
func (s *Server) Start() error {
	log.Debugf("starting server %d", s.Id)

	l, err := net.Listen(s.Self.Network, s.Self.Address)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Debugf("server %d listening on %s", s.Id, s.Self.Address)

	rpc.RegisterName("Server", s)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Errorf("server %d accept error: %v", s.Id, err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
