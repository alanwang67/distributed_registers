package client

import (
	"time"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type Client struct {
	Id      uint64
	Servers []*protocol.Connection
}

func New(id uint64, servers []*protocol.Connection) *Client {
	log.Debugf("client %d created", id)
	return &Client{
		Id:      id,
		Servers: servers,
	}
}

func (c *Client) Start() error {
	log.Debugf("starting client %d", c.Id)

	rc := 0 // retry count

	for {
		sc := len(c.Servers) // server count

		req := &protocol.ClientRequest{Id: c.Id}
		rep := &protocol.ClientReply{}

		log.Debugf("client %d sent a request", req.Id)
		protocol.Invoke(*c.Servers[rc%sc], "Server.HandleClientRequest", req, rep)
		log.Debugf("client %d received a reply from server %d with session ID %d", c.Id, rep.ServerId, rep.SessionId)
		rc += 1

		time.Sleep(250 * time.Millisecond)
	}
}
