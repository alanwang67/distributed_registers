package client

import (
	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/alanwang67/distributed_registers/server"
)

func communicateWithServer(clientData Client, value uint64) Client {
	for i := range clientData.Servers {
		clientReq := server.ClientRequest{OperationType: server.Write, SessionType: 0, Data: value,
			ReadVector:  make([]uint64, len(clientData.Servers)),
			WriteVector: make([]uint64, len(clientData.Servers))}

		req := server.Request{Type: server.Client, Client: clientReq}
		reply := server.Reply{}

		protocol.Invoke(*clientData.Servers[i], "Server.HandleNetworkCalls", &req, &reply)

		if reply.Client.Succeeded {
			return Client{Id: clientData.Id, Servers: clientData.Servers, ReadVector: reply.Client.ReadVector, WriteVector: reply.Client.WriteVector}
		}
	}

	panic("None found")
}
