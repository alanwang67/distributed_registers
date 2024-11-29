package client

import (
	"math/rand"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
	"github.com/charmbracelet/log"
)

// New creates a new Client instance.
func New(id uint64, servers []*protocol.Connection) *Client {
	log.Debugf("Client %d created", id)
	return &Client{
		Id:          id,
		Servers:     servers,
		ReadVector:  make([]uint64, len(servers)),
		WriteVector: make([]uint64, len(servers)),
	}
}

// Start initiates the client's operation loop.
func (c *Client) Start() error {
	log.Debugf("Starting client %d", c.Id)

	for {
		// Generate a random number from 0 to 100 w/ fixed seed for testing
		src := rand.NewSource(42)
		r := rand.New(src)
		valueToWrite := uint64(r.Intn(101))

		// Perform a write operation to the server
		success := c.writeValueToServer(valueToWrite)
		if success {
			log.Infof("Client %d wrote value: %d", c.Id, valueToWrite)
		} else {
			log.Errorf("Client %d failed to write value: %d", c.Id, valueToWrite)
		}

		// Perform a read operation to retrieve the latest value from the server
		serverValue := c.readValueFromServer()
		if serverValue != 0 {
			log.Infof("Client %d read value from server: %d", c.Id, serverValue)
		}

		// Sleep before the next operation
		time.Sleep(2500 * time.Millisecond)
	}
}

// writeValueToServer attempts to perform a write operation to the server with the given value.
// It updates the client's WriteVector based on the server's reply.
func (c *Client) writeValueToServer(value uint64) bool {
	// Iterate over the list of servers to find one that can process the request
	for _, serverConn := range c.Servers {

		clientReq := server.ClientRequest{
			OperationType: server.Write,
			SessionType:   server.Causal, // Requesting causal consistency
			Data:          value,
			ReadVector:    c.ReadVector,
			WriteVector:   c.WriteVector,
		}

		// Prepare a variable to receive the server's reply
		clientReply := server.ClientReply{}

		// Invoke the server's ProcessClientRequest method via RPC
		err := protocol.Invoke(*serverConn, "Server.ProcessClientRequest", &clientReq, &clientReply)
		if err != nil {
			log.Errorf("Error invoking server %s: %v", serverConn.Address, err)
			continue // Try the next server
		}

		// Check if the server successfully processed the request
		if clientReply.Succeeded {

			c.WriteVector = clientReply.WriteVector

			log.Infof("Client %d successfully wrote to server %s", c.Id, serverConn.Address)
			return true
		} else {
			log.Warnf("Server %s could not satisfy the write request due to dependency check failure", serverConn.Address)
			// Try the next server
		}
	}

	// If no servers were able to process the request, return false
	log.Errorf("Client %d: No servers were able to serve the write request", c.Id)
	return false
}

// readValueFromServer attempts to perform a read operation to get the value from the server.
// It updates the client's ReadVector based on the server's reply.
func (c *Client) readValueFromServer() uint64 {
	// Iterate over the list of servers to find one that can process the request
	for _, serverConn := range c.Servers {

		clientReq := server.ClientRequest{
			OperationType: server.Read,
			SessionType:   server.Causal, // Requesting causal consistency
			ReadVector:    c.ReadVector,
			WriteVector:   c.WriteVector,
		}

		// Prepare a variable to receive the server's reply
		clientReply := server.ClientReply{}

		// Invoke the server's ProcessClientRequest method via RPC
		err := protocol.Invoke(*serverConn, "Server.ProcessClientRequest", &clientReq, &clientReply)
		if err != nil {
			log.Errorf("Error invoking server %s: %v", serverConn.Address, err)
			continue // Try the next server
		}

		// Check if the server successfully processed the request
		if clientReply.Succeeded {

			c.ReadVector = clientReply.ReadVector

			log.Infof("Client %d successfully read from server %s", c.Id, serverConn.Address)
			return clientReply.Data // Return the value read from the server
		} else {
			log.Warnf("Server %s could not satisfy the read request due to dependency check failure", serverConn.Address)
			// Try the next server
		}
	}

	// If no servers were able to process the request, return 0 or handle accordingly
	log.Errorf("Client %d: No servers were able to serve the read request", c.Id)
	return 0
}
