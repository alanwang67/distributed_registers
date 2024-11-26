package client

import (
	"sync"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/server"
)

func (c *Client) communicateWithServer(ProposalNumber uint64, value uint64) bool {

	req := server.PrepareRequest{ProposalNumber: ProposalNumber}
	majority := (len(c.Servers) / 2) + 1

	voted := 0
	latestAcceptedProposalNumber := uint64(0)
	latestAcceptedProposalData := value
	var l sync.Mutex
	cond := sync.NewCond(&l)

	go func() {
		l.Lock()
		for voted < majority {
			cond.Wait()
		}
		l.Unlock()
		req := server.AcceptRequest{ProposalNumber: latestAcceptedProposalNumber, Value: latestAcceptedProposalData}
		rep := server.AcceptReply{}
		for i := range c.Servers {
			protocol.Invoke(*c.Servers[i], "Server.AcceptProposal", &req, &rep)
		}
	}()

	for i := range c.Servers {
		go func() {
			rep := server.PrepareReply{}
			protocol.Invoke(*c.Servers[i], "Server.PrepareRequest", &req, &rep)
			l.Lock()
			voted += 1
			if rep.LatestAcceptedProposalNumber > latestAcceptedProposalNumber {
				latestAcceptedProposalNumber = rep.LatestAcceptedProposalNumber
				latestAcceptedProposalData = rep.LatestAcceptedProposalData
			}
			l.Unlock()
			cond.Broadcast()
		}()
	}

	return true
}
