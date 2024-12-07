package sequencer

import (
	"github.com/alanwang67/distributed_registers/paxos/protocol"
)

// New creates and initializes a new Server instance with the given ID, self connection, and peer connections.
func New(self *protocol.Connection) *Sequencer {
	s := &Sequencer{
		Self:  self,
		Count: uint64(0),
	}
	return s
}

func (s *Sequencer) GetProposalNumber(_, reply *ReplyProposalNum) error {
	s.mu.Lock()
	reply.Count = s.Count
	s.Count += 1

	s.mu.Unlock()
	return nil
}
