package server

import (
	"net"
	"net/rpc"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type Operation struct {
	OperationType uint64 // 0 for read, 1 for write
	VersionVector []uint64
	Data          uint64
}

type ClientRequest struct {
	OperationType uint64 // 0 for read, 1 for write
	SessionType   uint64 // 0 for RYW, 1 for monotonic reads, 2 for WFR, 3 for monotonic writes
	Data          uint64 // only for write operations
	ReadVector    []uint64
	WriteVector   []uint64
}

type ClientReply struct {
	Succeeded     bool
	OperationType uint64
	Data          uint64
	ReadVector    []uint64
	WriteVector   []uint64
}

type ServerGossipRequest struct {
	ServerId   uint64
	Operations []Operation
}

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	VectorClock         []uint64
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
