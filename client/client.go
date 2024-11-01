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

	for {
		req := &protocol.ClientRequest{Id: c.Id}
		rep := &protocol.ClientReply{}
		protocol.Invoke(*c.Servers[0], "Server.HandleClientRequest", req, rep)
		log.Debugf("client %d sent request", req.Id)
		log.Debugf("client %d received reply", rep.SessionId)
		time.Sleep(250 * time.Millisecond)
	}
}
