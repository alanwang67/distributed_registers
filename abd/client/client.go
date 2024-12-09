package client

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

var versionCounter uint64 = 0
var versionMutex sync.Mutex // Mutex to protect versionCounter

// generateVersion generates a new, unique version number.
func generateVersion() uint64 {
	versionMutex.Lock()
	defer versionMutex.Unlock()
	versionCounter++
	return versionCounter
}

type InstructionType string

const (
	InstructionTypeRead  InstructionType = "read"
	InstructionTypeWrite InstructionType = "write"
)

type Instruction struct {
	Type  InstructionType `json:"Type"`
	Value *uint64         `json:"Value"` // Use a pointer to handle null values for reads
	Delay time.Duration   `json:"Delay"` // Delay in milliseconds
}

type ServerConfig struct {
	ID      uint64 `json:"id"`
	Network string `json:"network"`
	Address string `json:"address"`
}

type Client struct {
	Servers     []*ServerConfig
	Connections map[string]*rpc.Client
	mutex       sync.Mutex // Protects access to the connection pool
}

// NewClient initializes a client with server configurations and establishes connections.
func NewClient(servers []ServerConfig) (*Client, error) {
	serverPointers := make([]*ServerConfig, len(servers))
	connections := make(map[string]*rpc.Client)

	for i, server := range servers {
		serverPointers[i] = &server
		conn, err := rpc.Dial(server.Network, server.Address)
		if err != nil {
			fmt.Printf("Client: Failed to connect to server %s: %v\n", server.Address, err)
			continue
		}
		connections[server.Address] = conn
		fmt.Printf("Client: Connected to server %s\n", server.Address)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no servers available: all connection attempts failed")
	}

	client := &Client{
		Servers:     serverPointers,
		Connections: connections,
	}

	// Start monitoring connections to ensure reconnection if a server goes offline
	go client.monitorConnections()

	return client, nil
}

// monitorConnections checks server connectivity periodically and attempts to reconnect.
func (c *Client) monitorConnections() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, server := range c.Servers {
			c.mutex.Lock()
			_, connected := c.Connections[server.Address]
			c.mutex.Unlock()

			if !connected {
				fmt.Printf("Client: Attempting to reconnect to server %s\n", server.Address)
				conn, err := rpc.Dial(server.Network, server.Address)
				if err != nil {
					fmt.Printf("Client: Reconnection to server %s failed: %v\n", server.Address, err)
					continue
				}

				c.mutex.Lock()
				c.Connections[server.Address] = conn
				c.mutex.Unlock()
				fmt.Printf("Client: Reconnected to server %s\n", server.Address)
			}
		}
	}
}

// PerformOperation executes a single read or write operation on a connected server.
func (c *Client) PerformOperation(server *ServerConfig, instr Instruction) error {
	switch instr.Type {
	case InstructionTypeRead:
		// ABD read can remain as-is or implemented separately.
		return c.abdRead(server)
	case InstructionTypeWrite:
		if instr.Value == nil {
			return fmt.Errorf("write operation requires a value")
		}
		return c.abdWrite(*instr.Value)
	default:
		return fmt.Errorf("unknown instruction type: %s", instr.Type)
	}
}

// removeConnection removes a server's connection from the pool.
func (c *Client) removeConnection(address string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if conn, exists := c.Connections[address]; exists {
		conn.Close()
		delete(c.Connections, address)
		fmt.Printf("Client: Connection to server %s removed\n", address)
	}
}

// RunInteractiveMode starts the interactive prompt for the client.
func (c *Client) RunInteractiveMode(workload []Instruction) {
	reader := bufio.NewReader(os.Stdin)
	serverIndex := 0

	fmt.Println("Client: Enter '/start' to begin workload execution or '/exit' to quit.")

	for {
		fmt.Print("Client> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "/start":
			fmt.Println("Client: Starting workload execution...")
			for i, instr := range workload {
				server := c.Servers[serverIndex]

				fmt.Printf("Client: Executing operation %d: %v on server %s\n", i+1, instr, server.Address)
				err := c.PerformOperation(server, instr)
				if err != nil {
					fmt.Printf("Client: Operation %d failed on server %s: %v\n", i+1, server.Address, err)
				}

				// Rotate to the next server in round-robin fashion.
				serverIndex = (serverIndex + 1) % len(c.Servers)

				// Respect delay specified in the instruction.
				if instr.Delay > 0 {
					time.Sleep(instr.Delay)
				}
			}
			fmt.Println("Client: Workload execution completed.")
		case "/exit":
			fmt.Println("Client: Exiting and closing connections...")
			c.CloseConnections()
			return
		default:
			fmt.Println("Client: Unknown command. Use '/start' or '/exit'.")
		}
	}
}

// CloseConnections closes all connections to the servers.
func (c *Client) CloseConnections() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for addr, conn := range c.Connections {
		fmt.Printf("Client: Closing connection to server %s\n", addr)
		conn.Close()
	}
}

// abdWrite performs an ABD write operation.
func (c *Client) abdWrite(value uint64) error {
	version := generateVersion() // Use simpler version generation

	// Create the write request
	writeReq := struct {
		Version uint64
		Value   uint64
	}{
		Version: version,
		Value:   value,
	}

	// Process each server sequentially
	for _, srv := range c.Servers {
		// Acquire the mutex to safely check and use the connection
		c.mutex.Lock()
		conn, exists := c.Connections[srv.Address]
		c.mutex.Unlock()

		if !exists {
			fmt.Printf("Client: No active connection to server %s\n", srv.Address)
			continue
		}

		var reply struct{}
		err := conn.Call("Server.HandleWriteRequest", writeReq, &reply)
		if err != nil {
			// If an error occurs, remove the connection and log it
			c.removeConnection(srv.Address)
			fmt.Printf("Client: ABD Write Error - Server %s: %v\n", srv.Address, err)
			continue
		}

		fmt.Printf("Client: WRITE propagated to server %s - Version: %d, Value: %d\n",
			srv.Address, version, value)
	}

	fmt.Printf("Client: ABD Write completed - Version: %d, Value: %d\n", version, value)
	return nil
}

// abdRead (placeholder): This would contain the ABD read logic, if implemented.
// abdRead performs an ABD read operation.
func (c *Client) abdRead(server *ServerConfig) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	latestVersion := uint64(0)
	latestValue := uint64(0)
	errors := make(chan error, len(c.Servers))

	// Step 1: Query all servers for the latest version and value
	for _, srv := range c.Servers {
		wg.Add(1)
		go func(srv *ServerConfig) {
			defer wg.Done()

			c.mutex.Lock()
			conn, exists := c.Connections[srv.Address]
			c.mutex.Unlock()

			if !exists {
				errors <- fmt.Errorf("no active connection to server %s", srv.Address)
				return
			}

			var reply struct {
				Version uint64
				Value   uint64
			}
			err := conn.Call("Server.HandleReadRequest", struct{}{}, &reply)
			if err != nil {
				c.removeConnection(srv.Address)
				errors <- fmt.Errorf("read request failed to server %s: %v", srv.Address, err)
				return
			}

			mu.Lock()
			if reply.Version > latestVersion {
				latestVersion = reply.Version
				latestValue = reply.Value
			}
			mu.Unlock()

			fmt.Printf("Client: READ response from server %s - Version: %d, Value: %d\n", srv.Address, reply.Version, reply.Value)
		}(srv)
	}

	wg.Wait()
	close(errors)

	// Handle errors from read requests
	for err := range errors {
		fmt.Printf("Client: ABD Read Error - %v\n", err)
	}

	// Step 2: Send ReadConfirm to all servers with the latest version and value
	readConfirmReq := struct {
		Version uint64
		Value   uint64
	}{
		Version: latestVersion,
		Value:   latestValue,
	}

	for _, srv := range c.Servers {
		wg.Add(1)
		go func(srv *ServerConfig) {
			defer wg.Done()

			c.mutex.Lock()
			conn, exists := c.Connections[srv.Address]
			c.mutex.Unlock()

			if !exists {
				fmt.Printf("Client: Skipping ReadConfirm for server %s (no active connection)\n", srv.Address)
				return
			}

			var reply struct{}
			err := conn.Call("Server.HandleReadConfirm", readConfirmReq, &reply)
			if err != nil {
				c.removeConnection(srv.Address)
				fmt.Printf("Client: ReadConfirm failed to server %s: %v\n", srv.Address, err)
				return
			}

			fmt.Printf("Client: ReadConfirm sent to server %s - Version: %d, Value: %d\n", srv.Address, latestVersion, latestValue)
		}(srv)
	}

	wg.Wait()

	// Step 3: Return the read result to the user
	fmt.Printf("Client: ABD Read completed - Latest Version: %d, Latest Value: %d\n", latestVersion, latestValue)
	return nil
}

// getConnection retrieves or establishes a connection to the server.
func (c *Client) getConnection(server *ServerConfig) (*rpc.Client, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if conn, exists := c.Connections[server.Address]; exists {
		return conn, nil
	}

	// Establish a new connection if not available
	conn, err := rpc.Dial(server.Network, server.Address)
	if err != nil {
		return nil, err
	}
	c.Connections[server.Address] = conn
	return conn, nil
}
