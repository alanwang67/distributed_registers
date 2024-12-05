package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
	"github.com/charmbracelet/log"
	"golang.org/x/exp/rand"
)

type Client struct {
	Id          uint64
	Servers     []*protocol.Connection
	ReadVector  []uint64
	WriteVector []uint64
	mu          sync.Mutex
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

		resp := c.writeToServer(uint64(rand.Intn(500)), server.Causal)
		fmt.Print("write ", resp, "\n")
		resp = c.readFromServer(server.Causal)
		fmt.Print("read ", resp, "\n")

		rep := &protocol.ClientReply{}

		log.Debugf("client %d received a reply from server %d with session ID %d", c.Id, rep.ServerId, rep.SessionId)
		rc += 1

		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	for i := range c.Servers {
		clientReq := server.ClientRequest{}

		clientReply := server.ClientReply{}

		protocol.Invoke(*c.Servers[i], "Server.PrintOperations", &clientReq, &clientReply)

	}
	for {
		time.Sleep(100 * time.Microsecond)
	}

}
