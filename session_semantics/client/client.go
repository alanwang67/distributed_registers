package client

import (
	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
)

func (c *Client) communicateWithServer(value uint64) uint64 {
	for i := range c.Servers {
		clientReq := server.ClientRequest{OperationType: server.Write, SessionType: server.Causal, Data: value,
			ReadVector:  make([]uint64, len(c.Servers)),
			WriteVector: make([]uint64, len(c.Servers))}

		clientReply := server.ClientReply{}

		protocol.Invoke(*c.Servers[i], "Server.ProcessClientRequest", &clientReq, &clientReply)
		if clientReply.Succeeded {
			c.WriteVector = clientReply.WriteVector
			c.ReadVector = clientReply.ReadVector
			return clientReply.Data
		}
	}

	panic("No servers were able to serve your request")
}
