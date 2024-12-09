package sequencer

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/charmbracelet/log"
)

type Sequencer struct {
	Count uint64
	Self  *protocol.Connection
	mu    sync.Mutex
}

type ReqProposalNum struct {
}

type ReplyProposalNum struct {
	Count uint64
}

// New creates and initializes a new Sequencer instance with the given self connection.
func New(self *protocol.Connection) *Sequencer {
	s := &Sequencer{
		Self:  self,
		Count: uint64(1),
	}
	return s
}

// GetProposalNumber increments and returns the current proposal count.
func (s *Sequencer) GetProposalNumber(_ *ReqProposalNum, reply *ReplyProposalNum) error {
	s.mu.Lock()
	reply.Count = s.Count
	s.Count++
	s.mu.Unlock()
	log.Debugf("Sequencer returned proposal number %d", reply.Count)
	return nil
}

// Start begins listening for RPC requests on the sequencer's configured address.
func (s *Sequencer) Start() error {
	log.Debugf("starting sequencer")

	l, err := net.Listen(s.Self.Network, s.Self.Address)
	if err != nil {
		fmt.Print(err)
		return err
	}
	defer l.Close()
	log.Debugf("sequencer listening on %s", s.Self.Address)

	rpc.Register(s)

	for {
		rpc.Accept(l)
		// Other code could go here, if needed
	}
}
