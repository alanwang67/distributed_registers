package client

import (
	"time"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type Client struct {
	Id       uint64
	Servers  []*protocol.Connection
	ReadSet  []uint64
	WriteSet []uint64
}

type ClientRequest struct {
	OperationType uint64 // 0 for read, 1 for write
	SessionType   uint64 // 0 for RYW, 1 for monotonic reads, 2 for WFR, 3 for monotonic writes
	Data          uint64 // only for write operations
	ReadVector    []uint64
	WriteVector   []uint64
}

type ClientReply struct {
	Succeeded     bool
	OperationType uint64
	Data          uint64
	ReadVector    []uint64
	WriteVector   []uint64
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
