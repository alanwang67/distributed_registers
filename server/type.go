package server

import (
	"net"
	"net/rpc"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type LamportTS struct {
	Time       uint64
	TieBreaker uint64
}

type Operation struct {
	OperationType uint64 // 0 for read, 1 for write
	TimeStamp     LamportTS
	SessionId     uint64
	Data          uint64
}

type ClientRequest struct {
	OperationType uint64 // 0 for read, 1 for write
	SessionId     uint64
	Data          uint64 // only for write operations
}

// we can add more if we need for the session comparisons
type ClientReply struct {
	OperationType uint64
	TimeStamp     LamportTS
	Data          uint64 // only for read operations
}

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	LatestSeenLamportTS LamportTS
	OperationsPerformed []Operation
	Data                uint64
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
