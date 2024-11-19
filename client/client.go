package client

import (
	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/alanwang67/distributed_registers/server"
)

func (c *Client) communicateWithServer(value uint64) uint64 {
	for i := range c.Servers {
		clientReq := server.ClientRequest{OperationType: server.Write, SessionType: server.Causal, Data: value,
			ReadVector:  make([]uint64, len(c.Servers)),
			WriteVector: make([]uint64, len(c.Servers))}

		clientReply := server.ClientReply{}

		protocol.Invoke(*c.Servers[i], "Server.ProcessClientRequest", &clientReq, &clientReply)

		c.WriteVector = clientReply.WriteVector
		c.ReadVector = clientReply.ReadVector
		return clientReply.Data
	}

	panic("None found")
}
