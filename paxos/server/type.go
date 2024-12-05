package server

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/charmbracelet/log"
)

type Server struct {
	Id                           uint64
	Self                         *protocol.Connection
	Peers                        []*protocol.Connection
	Accepted                     bool
	LowestN                      uint64
	LatestAcceptedProposalNumber uint64
	LatestAcceptedProposalData   uint64
	mu                           sync.Mutex
}

type PrepareRequest struct {
	ProposalNumber uint64
}

type PrepareReply struct {
	ServerId                     uint64
	LatestAcceptedProposalNumber uint64
	LatestAcceptedProposalData   uint64
}

type AcceptRequest struct {
	ProposalNumber uint64
	Value          uint64
}

type AcceptReply struct {
	Succeeded bool
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
