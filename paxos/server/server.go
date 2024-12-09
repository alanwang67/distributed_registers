package server

import (
	"github.com/alanwang67/distributed_registers/paxos/protocol"
)

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

func (s *Server) PrepareRequest(request *PrepareRequest, reply *PrepareReply) error {
	s.mu.Lock()
	if s.LowestN < request.ProposalNumber {
		s.LowestN = request.ProposalNumber
	}

	if s.Accepted {
		reply.ServerId = s.Id
		reply.LatestAcceptedProposalNumber = s.LatestAcceptedProposalNumber
		reply.LatestAcceptedProposalData = s.LatestAcceptedProposalData
	}

	s.mu.Unlock()
	return nil
}

func (s *Server) AcceptProposal(request *AcceptRequest, reply *AcceptReply) error {
	s.mu.Lock()
	if s.LowestN <= request.ProposalNumber {
		s.LatestAcceptedProposalNumber = request.ProposalNumber
		s.LatestAcceptedProposalData = request.Value
	}

	s.Accepted = true
	reply.Succeeded = true
	s.mu.Unlock()
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
