package client

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
)

// New creates and initializes a new Client instance.
func New(id uint64, servers []*protocol.Connection) *Client {
	log.Printf("[DEBUG] client %d created", id)
	return &Client{
		Id:          id,
		Servers:     servers,
		ReadVector:  make([]uint64, len(servers)),
		WriteVector: make([]uint64, len(servers)),
	}
}

// Start executes client operations defined in the workload configuration file.
func (c *Client) Start(configPath string) error {
	log.Printf("[DEBUG] starting client %d", c.Id)

	// Load configuration file
	config, err := loadConfig(configPath)
	if err != nil {
		log.Printf("[ERROR] Failed to load config file: %v", err)
		return err
	}

	// Execute workload operations
	for _, op := range config.Workloads {
		c.mu.Lock()
		switch op.Type {
		case "read":
			resp := c.ReadFromServer(server.Causal)
			fmt.Printf("Client %d performed read operation: Response = %v\n", c.Id, resp)
		case "write":
			resp := c.WriteToServer(op.Value, server.Causal)
			fmt.Printf("Client %d performed write operation with value %d: Response = %v\n", c.Id, op.Value, resp)
		default:
			log.Printf("[WARN] Unknown operation type: %s", op.Type)
		}
		c.mu.Unlock()

		// Apply delay if specified
		if op.Delay > 0 {
			time.Sleep(time.Duration(op.Delay) * time.Millisecond)
		}
	}

	// Pause and then fetch operations from servers
	time.Sleep(500 * time.Millisecond)
	for i := range c.Servers {
		clientReq := server.ClientRequest{}
		clientReply := server.ClientReply{}
		protocol.Invoke(*c.Servers[i], "Server.PrintOperations", &clientReq, &clientReply)
		fmt.Printf("Client %d fetched operations from server %d\n", c.Id, i)
	}

	// Keep the client running indefinitely
	for {
		time.Sleep(100 * time.Microsecond)
	}
}

// loadConfig reads and parses the workload configuration from a JSON file.
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("could not parse config file: %w", err)
	}

	return &config, nil
}

// WriteToServer performs a write operation on a server with the specified session type.
func (c *Client) WriteToServer(value uint64, sessionSemantic server.SessionType) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	order := rand.Perm(len(c.Servers))
	for _, v := range order {
		clientReq := server.ClientRequest{
			OperationType: server.Write,
			SessionType:   sessionSemantic,
			Data:          value,
			ReadVector:    c.ReadVector,
			WriteVector:   c.WriteVector,
		}

		clientReply := server.ClientReply{}

		// Invoke the server method
		protocol.Invoke(*c.Servers[v], "Server.ProcessClientRequest", &clientReq, &clientReply)

		if clientReply.Succeeded {
			// Update client vectors if the operation succeeded
			c.WriteVector = clientReply.WriteVector
			c.ReadVector = clientReply.ReadVector
			return clientReply.Data
		}
	}

	// Panic if no servers could handle the request
	panic("No servers were able to serve your request")
}

// ReadFromServer performs a read operation on a server with the specified session type.
func (c *Client) ReadFromServer(sessionSemantic server.SessionType) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	order := rand.Perm(len(c.Servers))
	for _, v := range order {
		clientReq := server.ClientRequest{
			OperationType: server.Read,
			SessionType:   sessionSemantic,
			ReadVector:    c.ReadVector,
			WriteVector:   c.WriteVector,
		}

		clientReply := server.ClientReply{}

		// Invoke the server method
		protocol.Invoke(*c.Servers[v], "Server.ProcessClientRequest", &clientReq, &clientReply)

		if clientReply.Succeeded {
			// Update client vectors if the operation succeeded
			c.WriteVector = clientReply.WriteVector
			c.ReadVector = clientReply.ReadVector
			return clientReply.Data
		}
	}

	// Panic if no servers could handle the request
	panic("No servers were able to serve your request")
}
