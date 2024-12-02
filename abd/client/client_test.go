package client

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"github.com/alanwang67/distributed_registers/abd/workload"
	"github.com/charmbracelet/log"
)

// Helper function to start a server instance for testing.
func startTestServer(t *testing.T, id uint64, address string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create a server instance
	srv := server.New(id, &protocol.Connection{
		Network: "tcp",
		Address: address,
	}, nil)

	// Start the server in a separate goroutine
	go func() {
		err := srv.Start()
		if err != nil {
			t.Errorf("Server %d encountered an error: %v", id, err)
		}
	}()
	// Give the server some time to start
	time.Sleep(100 * time.Millisecond)
}

// TestClientABDOperations tests the ABDRead and ABDWrite operations of the Client.
func TestClientABDOperations(t *testing.T) {
	// Set up logging level
	log.SetLevel(log.DebugLevel)

	// Start multiple servers
	numServers := 5
	var wg sync.WaitGroup
	serverAddresses := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		wg.Add(1)
		address := "127.0.0.1:" + strconv.Itoa(8000+i)
		serverAddresses[i] = address
		go startTestServer(t, uint64(i), address, &wg)
	}
	// Wait for all servers to start
	wg.Wait()

	// Create protocol connections for servers
	servers := make([]*protocol.Connection, numServers)
	for i, addr := range serverAddresses {
		servers[i] = &protocol.Connection{
			Network: "tcp",
			Address: addr,
		}
	}

	// Create a client connected to the servers
	clientID := uint64(1)
	c := New(clientID, servers)

	// Test ABDWrite operation
	writeValue := uint64(42)
	err := c.ABDWrite(writeValue)
	if err != nil {
		t.Fatalf("ABDWrite failed: %v", err)
	}

	// Verify that the client's LocalVersion is updated
	if c.LocalVersion == 0 {
		t.Errorf("Client LocalVersion not updated after ABDWrite")
	}

	// Test ABDRead operation
	readValue, err := c.ABDRead()
	if err != nil {
		t.Fatalf("ABDRead failed: %v", err)
	}

	// Verify that the read value matches the written value
	if readValue != writeValue {
		t.Errorf("ABDRead returned %d, expected %d", readValue, writeValue)
	}

	// Clean up servers
	for _, addr := range serverAddresses {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		_ = conn.Close()
	}
}

// TestClientStart tests the Start method of the Client with a sample workload.
func TestClientStart(t *testing.T) {
	// Set up logging level
	log.SetLevel(log.DebugLevel)

	// Start multiple servers
	numServers := 5
	var wg sync.WaitGroup
	serverAddresses := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		wg.Add(1)
		address := "127.0.0.1:" + strconv.Itoa(8100+i)
		serverAddresses[i] = address
		go startTestServer(t, uint64(i), address, &wg)
	}
	// Wait for all servers to start
	wg.Wait()

	// Create protocol connections for servers
	servers := make([]*protocol.Connection, numServers)
	for i, addr := range serverAddresses {
		servers[i] = &protocol.Connection{
			Network: "tcp",
			Address: addr,
		}
	}

	// Create a client connected to the servers
	clientID := uint64(2)
	c := New(clientID, servers)

	// Generate a sample workload
	workloadInstructions := []workload.Instruction{
		{Type: workload.InstructionTypeWrite, Value: 100},
		{Type: workload.InstructionTypeRead},
		{Type: workload.InstructionTypeWrite, Value: 200},
		{Type: workload.InstructionTypeRead},
	}

	// Start the client with the workload
	err := c.Start(workloadInstructions)
	if err != nil {
		t.Fatalf("Client Start failed: %v", err)
	}

	// Clean up servers
	for _, addr := range serverAddresses {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		_ = conn.Close()
	}
}

// TestConcurrentClients tests the scenario with multiple clients performing operations concurrently.
func TestConcurrentClients(t *testing.T) {
	// Set up logging level
	log.SetLevel(log.DebugLevel)

	// Start multiple servers
	numServers := 5
	var wg sync.WaitGroup
	serverAddresses := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		wg.Add(1)
		address := "127.0.0.1:" + strconv.Itoa(8200+i)
		serverAddresses[i] = address
		go startTestServer(t, uint64(i), address, &wg)
	}
	// Wait for all servers to start
	wg.Wait()

	// Create protocol connections for servers
	servers := make([]*protocol.Connection, numServers)
	for i, addr := range serverAddresses {
		servers[i] = &protocol.Connection{
			Network: "tcp",
			Address: addr,
		}
	}

	// Define a function for client operations
	clientFunc := func(clientID uint64, t *testing.T) {
		c := New(clientID, servers)
		// Each client performs a write and a read
		writeValue := clientID * 100
		err := c.ABDWrite(writeValue)
		if err != nil {
			t.Errorf("Client %d ABDWrite failed: %v", clientID, err)
			return
		}
		readValue, err := c.ABDRead()
		if err != nil {
			t.Errorf("Client %d ABDRead failed: %v", clientID, err)
			return
		}
		// The read value may not match the write value due to concurrent writes
		// So we log the read value for debugging purposes
		t.Logf("Client %d read value: %d", clientID, readValue)
	}

	// Start multiple clients concurrently
	numClients := 3
	var clientWg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		clientWg.Add(1)
		go func(clientID uint64) {
			defer clientWg.Done()
			clientFunc(clientID, t)
		}(uint64(i + 1))
	}

	// Wait for all clients to finish
	clientWg.Wait()

	// Clean up servers
	for _, addr := range serverAddresses {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		_ = conn.Close()
	}
}

// TestReadWriteConsistency tests the consistency of read and write operations.
func TestReadWriteConsistency(t *testing.T) {
	// Set up logging level
	log.SetLevel(log.DebugLevel)

	// Start multiple servers
	numServers := 5
	var wg sync.WaitGroup
	serverAddresses := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		wg.Add(1)
		address := "127.0.0.1:" + strconv.Itoa(8300+i)
		serverAddresses[i] = address
		go startTestServer(t, uint64(i), address, &wg)
	}
	// Wait for all servers to start
	wg.Wait()

	// Create protocol connections for servers
	servers := make([]*protocol.Connection, numServers)
	for i, addr := range serverAddresses {
		servers[i] = &protocol.Connection{
			Network: "tcp",
			Address: addr,
		}
	}

	// Create a client connected to the servers
	clientID := uint64(3)
	c := New(clientID, servers)

	// Perform a write
	writeValue := uint64(500)
	err := c.ABDWrite(writeValue)
	if err != nil {
		t.Fatalf("ABDWrite failed: %v", err)
	}

	// Perform a read
	readValue, err := c.ABDRead()
	if err != nil {
		t.Fatalf("ABDRead failed: %v", err)
	}

	// Verify that the read value matches the written value
	if readValue != writeValue {
		t.Errorf("ABDRead returned %d, expected %d", readValue, writeValue)
	}

	// Simulate a new client reading the value
	newClient := New(clientID+1, servers)
	newReadValue, err := newClient.ABDRead()
	if err != nil {
		t.Fatalf("New client ABDRead failed: %v", err)
	}

	// Verify that the new client reads the same value
	if newReadValue != writeValue {
		t.Errorf("New client ABDRead returned %d, expected %d", newReadValue, writeValue)
	}

	// Clean up servers
	for _, addr := range serverAddresses {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		_ = conn.Close()
	}
}
