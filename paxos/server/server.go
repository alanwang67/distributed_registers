package server

import (
	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
)

// New creates and initializes a new Server instance with the given ID, self connection, and peer connections.
func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	s := &Server{
		Id:                           id,
		Self:                         self,
		Peers:                        peers,
		Accepted:                     true,
		LowestN:                      0,
		LatestAcceptedProposalNumber: 0,
		LatestAcceptedProposalData:   0,
	}
	return s
}

func (s *Server) prepareRequest(request PrepareRequest, reply PrepareReply) error {
	if s.LowestN < request.ProposalNumber {
		s.LowestN = request.ProposalNumber
	}

	if s.Accepted {
		reply.ServerId = s.Id
		reply.LatestAcceptedProposalNumber = s.LatestAcceptedProposalNumber
		reply.LatestAcceptedProposalData = s.LatestAcceptedProposalData

	}
	return nil
}

func (s *Server) acceptProposal(request AcceptRequest, reply AcceptReply) error {
	if s.LowestN <= request.ProposalNumber {
		s.LatestAcceptedProposalData = request.ProposalNumber
		s.LatestAcceptedProposalData = request.Value
	}

	return nil
}
