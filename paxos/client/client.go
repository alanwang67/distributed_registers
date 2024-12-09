package client

import (
	"sync"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/sequencer"
	"github.com/alanwang67/distributed_registers/paxos/server"
)

func (c *Client) writeOperation(ProposalNumber uint64, value uint64) bool {
	req := server.PrepareRequest{ProposalNumber: ProposalNumber}
	majority := (len(c.Servers) / 2) + 1

	voted := 0
	latestAcceptedProposalNumber := uint64(0)
	latestAcceptedProposalData := value
	var l sync.Mutex
	cond := sync.NewCond(&l)

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

	l.Lock()
	for voted < majority {
		cond.Wait()
	}

	l.Unlock()

	acceptReq := server.AcceptRequest{ProposalNumber: ProposalNumber, Value: latestAcceptedProposalData}
	acceptRep := server.AcceptReply{}

	for i := range c.Servers {
		protocol.Invoke(*c.Servers[i], "Server.AcceptProposal", &acceptReq, &acceptRep)
	}

	return true
}

func determineMajority(arr []uint64, total uint64) bool {
	m := make(map[uint64]uint64)
	for _, v := range arr {
		m[v] += 1
	}
	for k, v := range m {
		if k != 0 && v >= total {
			return true
		}
	}
	return false
}

func getMajority(arr []uint64) uint64 {
	m := make(map[uint64]uint64)
	for _, v := range arr {
		m[v] += 1
	}

	var result uint64
	var occurences uint64 = 0
	for k, v := range m {
		if v >= occurences && k != 0 {
			result = k
			occurences = v
		}
	}
	return result
}

func (c *Client) readOperation() uint64 {
	ct := 0
	values := make([]uint64, 0)
	m := make(map[uint64]uint64)
	var l sync.Mutex
	cond := sync.NewCond(&l)

	for i := range c.Servers {
		go func() {
			req := server.ReadRequest{}
			rep := server.ReadReply{}
			protocol.Invoke(*c.Servers[i], "Server.QuorumRead", &req, &rep)
			l.Lock()
			ct += 1
			values = append(values, rep.ProposalNumber)
			m[rep.ProposalNumber] = rep.Value
			l.Unlock()
			cond.Broadcast()
		}()
	}

	majority := (len(c.Servers) / 2) + 1

	l.Lock()

	for !determineMajority(values, uint64(majority)) || ct < majority {
		cond.Wait()
	}

	r := getMajority(values)
	// fmt.Print(m, "", "\n")
	retValue := m[r]
	b := determineMajority(values, uint64(majority))

	l.Unlock()

	if !b {
		req := sequencer.ReqProposalNum{}
		rep := sequencer.ReplyProposalNum{}
		protocol.Invoke(*c.Sequencers[0], "Sequencer.GetProposalNumber", &req, &rep)
		c.writeOperation(rep.Count, retValue)
	}

	return retValue
}
