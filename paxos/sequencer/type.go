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
		// some other stuff goes here...

	}
}
