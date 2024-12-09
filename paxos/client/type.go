package client

import (
	"fmt"
	"time"

	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/sequencer"

	"github.com/charmbracelet/log"
)

type Client struct {
	Id         uint64
	Servers    []*protocol.Connection
	Sequencers []*protocol.Connection
}

func New(id uint64, servers []*protocol.Connection, sequencers []*protocol.Connection) *Client {
	log.Debugf("client %d created", id)
	return &Client{
		Id:         id,
		Servers:    servers,
		Sequencers: sequencers,
	}
}

func (c *Client) Start() error {
	log.Debugf("starting client %d", c.Id)
	rc := uint64(0)

	for rc < 100 {
		req := sequencer.ReqProposalNum{}
		rep := sequencer.ReplyProposalNum{}
		protocol.Invoke(*c.Sequencers[0], "Sequencer.GetProposalNumber", &req, &rep)
		fmt.Print(rep)
		// send rpc to someone to get proposal number
		resp := c.writeOperation(rep.Count, rc)
		fmt.Print("proposal number: ", rep.Count, "value written: ", resp, "\n")
		read := c.readOperation()
		fmt.Print("value read: ", read, "\n")
		rc += 1
	}
	for {
		time.Sleep(100 * time.Millisecond)
	}
}
