package server

import (
	"math/rand/v2"
	"net"
	"net/rpc"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection
}

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	return &Server{
		Id:    id,
		Self:  self,
		Peers: peers,
	}
}

func (s *Server) Start() error {
	log.Debugf("starting server %d", s.Id)

	l, err := net.Listen(s.Self.Network, s.Self.Address)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Debugf("server %d listening on %s", s.Id, s.Self.Address)

	rpc.Register(s)

	for {
		rpc.Accept(l)
		// some other stuff goes here...
	}
}

func (s *Server) HandleClientRequest(req *protocol.ClientRequest, reply *protocol.ClientReply) error {
	log.Debugf("server %d handling client request %d", s.Id, req.Id)

	*reply = protocol.ClientReply{SessionId: uint64(rand.Uint32())<<32 + uint64(rand.Uint32())}
	log.Debugf("server %d replied to client %d with session %d", s.Id, req.Id, reply.SessionId)

	return nil
}
