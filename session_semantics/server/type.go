package server

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/charmbracelet/log"
)

type RequestType uint64

const (
	Client RequestType = iota
	Gossip
)

type OperationType uint64

const (
	Read OperationType = iota
	Write
)

type SessionType uint64

const (
	Causal SessionType = iota
	MonotonicReads
	MonotonicWrites
	ReadYourWrites
	WritesFollowReads
)

type Operation struct {
	OperationType OperationType
	VersionVector []uint64
	TieBreaker    uint64
	Data          uint64
}

type ClientRequest struct {
	OperationType OperationType
	SessionType   SessionType
	Data          uint64
	ReadVector    []uint64
	WriteVector   []uint64
}

type ClientReply struct {
	Succeeded     bool
	OperationType OperationType
	Data          uint64
	ReadVector    []uint64
	WriteVector   []uint64
}

type GossipRequest struct {
	ServerId   uint64
	Operations []Operation
}

type GossipReply struct {
}

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	VectorClock         []uint64
	OperationsPerformed []Operation
	MyOperations        []Operation
	PendingOperations   []Operation
	Data                uint64
	mu                  sync.Mutex
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
