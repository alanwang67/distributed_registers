package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/charmbracelet/log"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/sequencer"
	"github.com/alanwang67/distributed_registers/paxos/server"
)

type Client struct {
	Id         uint64
	Servers    []*protocol.Connection
	Sequencers []*protocol.Connection

	chosen    bool
	chosenVal uint64
}

func New(id uint64, servers []*protocol.Connection, sequencers []*protocol.Connection) *Client {
	log.Debugf("client %d created", id)
	return &Client{
		Id:         id,
		Servers:    servers,
		Sequencers: sequencers,
		chosen:     false,
		chosenVal:  0,
	}
}

func invokeSafe(conn protocol.Connection, method string, args, reply any) error {
	start := time.Now()
	err := protocol.Invoke(conn, method, args, reply)
	elapsed := time.Since(start)
	if err != nil {
		log.Errorf("RPC call %s to %s failed (took %v): %v", method, conn.Address, elapsed, err)
	} else {
		log.Debugf("RPC call %s to %s succeeded (took %v)", method, conn.Address, elapsed)
	}
	return err
}

func (c *Client) Start() error {
	time.Sleep(500 * time.Millisecond)
	log.Infof("starting client %d", c.Id)

	maxWrites := 10
	retries := 0
	const valueToWrite = 42 // Always write the same value

	for i := 0; i < maxWrites && !c.chosen; i++ {
		req := sequencer.ReqProposalNum{}
		rep := sequencer.ReplyProposalNum{}

		getPropStart := time.Now()
		err := invokeSafe(*c.Sequencers[0], "Sequencer.GetProposalNumber", &req, &rep)
		log.Debugf("Client %d: GetProposalNumber took %v", c.Id, time.Since(getPropStart))
		if err != nil || rep.Count == 0 {
			log.Errorf("failed to get valid proposal number, retrying...")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		log.Infof("Client %d attempting write with proposal %d, value %d", c.Id, rep.Count, valueToWrite)
		writeStart := time.Now()
		if !c.writeOperation(rep.Count, valueToWrite) {
			log.Warnf("Client %d: writeOperation failed, took %v", c.Id, time.Since(writeStart))
			retries++
			if retries >= 3 {
				log.Errorf("Client %d: writeOperation failed after 3 attempts, aborting writes.", c.Id)
				break
			}
			log.Warnf("writeOperation failed, retrying... (%d/3)", retries)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		log.Infof("Client %d: writeOperation succeeded in %v", c.Id, time.Since(writeStart))

		// Write succeeded
		retries = 0
		c.chosen = true
		c.chosenVal = valueToWrite
		log.Infof("Client %d: Value %d chosen!", c.Id, c.chosenVal)

		// Perform a few reads to check the stable majority
		for j := 0; j < 3; j++ {
			readStart := time.Now()
			val := c.readOperation()
			log.Infof("Client %d read quorum value: %d (took %v)", c.Id, val, time.Since(readStart))
			fmt.Printf("value read: %d\n", val)
			time.Sleep(200 * time.Millisecond)
		}

		break
		time.Sleep(100 * time.Millisecond)
	}

	// Keep running to allow observation
	for {
		time.Sleep(1 * time.Second)
	}
}

func (c *Client) writeOperation(ProposalNumber uint64, value uint64) bool {
	req := server.PrepareRequest{ProposalNumber: ProposalNumber}
	majority := (len(c.Servers) / 2) + 1

	voted := 0
	latestAcceptedProposalNumber := uint64(0)
	latestAcceptedProposalData := value
	var l sync.Mutex
	cond := sync.NewCond(&l)

	log.Debugf("Client %d: Starting writeOperation with ProposalNumber=%d, Value=%d", c.Id, ProposalNumber, value)
	prepareStart := time.Now()

	// Prepare phase
	for i := range c.Servers {
		i := i
		go func() {
			rep := server.PrepareReply{}
			err := invokeSafe(*c.Servers[i], "Server.PrepareRequest", &req, &rep)
			l.Lock()
			if err == nil {
				voted++
				if rep.LatestAcceptedProposalNumber > latestAcceptedProposalNumber {
					latestAcceptedProposalNumber = rep.LatestAcceptedProposalNumber
					latestAcceptedProposalData = rep.LatestAcceptedProposalData
				}
			}
			l.Unlock()
			cond.Broadcast()
		}()
	}

	l.Lock()
	deadline := time.Now().Add(1 * time.Second)
	for voted < majority {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			l.Unlock()
			log.Errorf("writeOperation timed out waiting for prepare majority (proposal %d)", ProposalNumber)
			return false
		}
		cond.Wait()
	}
	l.Unlock()

	if voted < majority {
		log.Errorf("writeOperation: no majority in prepare phase for proposal %d", ProposalNumber)
		return false
	}
	log.Debugf("writeOperation: prepare majority reached for proposal %d, proposing value %d (prepare took %v)",
		ProposalNumber, latestAcceptedProposalData, time.Since(prepareStart))

	// Accept phase
	acceptStart := time.Now()
	acceptReq := server.AcceptRequest{ProposalNumber: ProposalNumber, Value: latestAcceptedProposalData}
	acceptCount := 0
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range c.Servers {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			accRep := server.AcceptReply{}
			err := invokeSafe(*c.Servers[i], "Server.AcceptProposal", &acceptReq, &accRep)
			if err == nil && accRep.Succeeded {
				mu.Lock()
				acceptCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if acceptCount < majority {
		log.Errorf("writeOperation: no majority in accept phase for proposal %d (needed %d got %d)",
			ProposalNumber, majority, acceptCount)
		return false
	}

	log.Debugf("writeOperation: accept majority reached for proposal %d (accept took %v)", ProposalNumber, time.Since(acceptStart))
	return true
}

func determineMajority(arr []uint64, total uint64) bool {
	m := make(map[uint64]uint64)
	for _, v := range arr {
		m[v]++
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
		m[v]++
	}

	var result uint64
	var occurrences uint64
	for k, v := range m {
		if v >= occurrences && k != 0 {
			result = k
			occurrences = v
		}
	}
	return result
}

func (c *Client) readOperation() uint64 {
	readStart := time.Now()
	majority := (len(c.Servers) / 2) + 1
	ct := 0
	values := make([]uint64, 0)
	m := make(map[uint64]uint64)
	var l sync.Mutex
	cond := sync.NewCond(&l)

	log.Debugf("Client %d: Starting readOperation", c.Id)
	for i := range c.Servers {
		i := i
		go func() {
			req := server.ReadRequest{}
			rep := server.ReadReply{}
			err := invokeSafe(*c.Servers[i], "Server.QuorumRead", &req, &rep)
			l.Lock()
			if err == nil {
				ct++
				values = append(values, rep.ProposalNumber)
				m[rep.ProposalNumber] = rep.Value
			}
			l.Unlock()
			cond.Broadcast()
		}()
	}

	l.Lock()
	deadline := time.Now().Add(1 * time.Second)
	for {
		if ct >= majority && determineMajority(values, uint64(majority)) {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			l.Unlock()
			log.Errorf("readOperation: timed out waiting for majority read (took %v)", time.Since(readStart))
			return 0
		}
		cond.Wait()
	}

	r := getMajority(values)
	retValue := m[r]
	b := determineMajority(values, uint64(majority))
	l.Unlock()

	if !b {
		// No stable majority: attempt stabilization
		log.Debugf("readOperation: no stable majority found, attempting stabilization write with value %d (read took %v so far)",
			retValue, time.Since(readStart))
		req := sequencer.ReqProposalNum{}
		rep := sequencer.ReplyProposalNum{}
		err := invokeSafe(*c.Sequencers[0], "Sequencer.GetProposalNumber", &req, &rep)
		if err == nil {
			stabStart := time.Now()
			if !c.writeOperation(rep.Count, retValue) {
				log.Errorf("readOperation: stabilization write failed (attempted after %v total read time)", time.Since(readStart))
			} else {
				log.Debugf("readOperation: stabilization write succeeded (stabilization took %v, total read time %v)",
					time.Since(stabStart), time.Since(readStart))
			}
		} else {
			log.Errorf("readOperation: failed to get new proposal number for stabilization: %v", err)
		}
	} else {
		log.Debugf("readOperation: stable majority read with value %d (took %v)", retValue, time.Since(readStart))
	}

	return retValue
}
