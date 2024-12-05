package client

import (
	"math/rand/v2"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
)

func (c *Client) writeToServer(value uint64, sessionSemantic server.SessionType) uint64 {
	c.mu.Lock()
	order := rand.Perm(len(c.Servers))
	for _, v := range order {
		clientReq := server.ClientRequest{OperationType: server.Write, SessionType: sessionSemantic, Data: value,
			ReadVector:  c.ReadVector,
			WriteVector: c.WriteVector}

		clientReply := server.ClientReply{}
		c.mu.Unlock()

		protocol.Invoke(*c.Servers[v], "Server.ProcessClientRequest", &clientReq, &clientReply)
		c.mu.Lock()
		if clientReply.Succeeded {
			c.WriteVector = clientReply.WriteVector
			c.ReadVector = clientReply.ReadVector
			c.mu.Unlock()
			return clientReply.Data
		}
	}
	c.mu.Unlock()

	panic("No servers were able to serve your request") // This shouldn't happen if none of the servers fail
}

func (c *Client) readFromServer(sessionSemantic server.SessionType) uint64 {
	c.mu.Lock()
	order := rand.Perm(len(c.Servers))
	for _, v := range order {
		clientReq := server.ClientRequest{OperationType: server.Read, SessionType: sessionSemantic,
			ReadVector:  c.ReadVector,
			WriteVector: c.WriteVector}

		c.mu.Unlock()
		clientReply := server.ClientReply{}

		protocol.Invoke(*c.Servers[v], "Server.ProcessClientRequest", &clientReq, &clientReply)

		c.mu.Lock()
		if clientReply.Succeeded {
			c.WriteVector = clientReply.WriteVector
			c.ReadVector = clientReply.ReadVector
			c.mu.Unlock()
			return clientReply.Data
		}
	}
	c.mu.Unlock()

	panic("No servers were able to serve your request") // This shouldn't happen if none of the servers fail
}
