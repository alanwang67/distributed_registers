package server

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
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

type ReadRequest struct {
}

type ReadReply struct {
	Value          uint64
	ProposalNumber uint64
}

// New creates and initializes a new Server instance with the given ID, self connection, and peer connections.
func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	s := &Server{
		Id:                           id,
		Self:                         self,
		Peers:                        peers,
		Accepted:                     false,
		LowestN:                      0,
		LatestAcceptedProposalNumber: 0,
		LatestAcceptedProposalData:   0,
	}
	return s
}

func (s *Server) HandleClientRequest(req *protocol.ClientRequest, reply *protocol.ClientReply) error {
	log.Printf("[DEBUG] server %d handling client request %d", s.Id, req.Id)

	*reply = protocol.ClientReply{
		ServerId:  s.Id,
		SessionId: uint64(rand.Uint32())<<32 + uint64(rand.Uint32()),
	}
	log.Printf("[DEBUG] server %d replied to client %d with session %d", s.Id, req.Id, reply.SessionId)

	return nil
}

func (s *Server) PrepareRequest(request *PrepareRequest, reply *PrepareReply) error {
	s.mu.Lock()
	if s.LowestN < request.ProposalNumber {
		s.LowestN = request.ProposalNumber
	}

	reply.ServerId = s.Id

	if s.Accepted {
		reply.LatestAcceptedProposalNumber = s.LatestAcceptedProposalNumber
		reply.LatestAcceptedProposalData = s.LatestAcceptedProposalData
	}
	s.mu.Unlock()

	return nil
}

func (s *Server) AcceptProposal(request *AcceptRequest, reply *AcceptReply) error {
	s.mu.Lock()
	log.Printf("[DEBUG] Server %d received AcceptProposal (N=%d, value=%d)", s.Id, request.ProposalNumber, request.Value)
	if s.LowestN <= request.ProposalNumber {
		s.LatestAcceptedProposalNumber = request.ProposalNumber
		s.LatestAcceptedProposalData = request.Value
	}
	s.Accepted = true
	reply.Succeeded = true
	s.mu.Unlock()
	log.Printf("[DEBUG] Server %d accepted proposal %d with value %d", s.Id, request.ProposalNumber, request.Value)
	return nil
}

func (s *Server) QuorumRead(request *ReadRequest, reply *ReadReply) error {
	s.mu.Lock()
	if s.LatestAcceptedProposalData > 0 {
		reply.Value = s.LatestAcceptedProposalData
		reply.ProposalNumber = s.LatestAcceptedProposalNumber
	}
	s.mu.Unlock()

	return nil
}

func (s *Server) Start() error {
	log.Printf("[DEBUG] starting server %d", s.Id)

	l, err := net.Listen(s.Self.Network, s.Self.Address)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Printf("[DEBUG] server %d listening on %s", s.Id, s.Self.Address)

	rpc.Register(s)

	for {
		rpc.Accept(l)
		// some other stuff goes here...
	}
}
