package client

import (
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
	"github.com/charmbracelet/log"
)

type Client struct {
	Id          uint64
	Servers     []*protocol.Connection
	ReadVector  []uint64
	WriteVector []uint64
}

func New(id uint64, servers []*protocol.Connection) *Client {
	log.Debugf("client %d created", id)
	return &Client{
		Id:          id,
		Servers:     servers,
		ReadVector:  make([]uint64, len(servers)),
		WriteVector: make([]uint64, len(servers)),
	}
}

func (c *Client) Start() error {
	log.Debugf("starting client %d", c.Id)

	rc := uint64(0) // retry count

	for rc < 20 {

		resp := c.communicateWithServer(rc)
		log.Debugf("%d", resp)
		// sc := len(c.Servers) // server count

		// req := &protocol.ClientRequest{Id: c.Id}
		rep := &protocol.ClientReply{}

		// log.Debugf("client %d sent a request", req.Id)
		// protocol.Invoke(*c.Servers[rc%sc], "Server.HandleClientRequest", req, rep)
		log.Debugf("client %d received a reply from server %d with session ID %d", c.Id, rep.ServerId, rep.SessionId)
		rc += 1

		time.Sleep(250 * time.Millisecond)
	}

	for i := range c.Servers {
		clientReq := server.ClientRequest{}

		clientReply := server.ClientReply{}

		protocol.Invoke(*c.Servers[i], "Server.PrintOperations", &clientReq, &clientReply)

	}
	for {
		time.Sleep(100 * time.Microsecond)
	}

}
