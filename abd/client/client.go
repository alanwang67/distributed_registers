package client

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"github.com/alanwang67/distributed_registers/abd/workload"
	"github.com/charmbracelet/log"
)

// New creates a new Client with the given ID and list of servers.
func New(id uint64, servers []*protocol.Connection) *Client {
	log.Debugf("client %d created", id)
	return &Client{
		Id:           id,
		Servers:      servers,
		LocalVersion: 0,
	}
}

// ABDRead performs the ABD Read operation.
// It fetches the highest version number and associated value from the quorum.
// Returns the value with the highest version or an error if the quorum is not achieved.
func (c *Client) ABDRead() (uint64, error) {
	readReplies, err := c.readFromQuorum()
	if err != nil {
		return 0, err
	}

	// Find the highest version number and associated value
	maxVersion := c.LocalVersion
	var value uint64
	for _, reply := range readReplies {
		if reply.Version > maxVersion {
			maxVersion = reply.Version
			value = reply.Value
		}
	}

	// Update LocalVersion for session guarantees
	c.LocalVersion = maxVersion

	// Optional Write-Back Phase
	err = c.writeToQuorum(maxVersion, value) // Broadcast the highest value back to a quorum
	if err != nil {
		return 0, fmt.Errorf("write-back failed: %v", err)
	}

	return value, nil
}

// ABDWrite performs the ABD Write operation.
// It writes a new value with an incremented version number to the quorum.
func (c *Client) ABDWrite(value uint64) error {
	// Phase 1: Read from quorum to get the highest version
	readReplies, err := c.readFromQuorum()
	if err != nil {
		return err
	}

	// Determine the highest version number
	maxVersion := c.LocalVersion
	for _, reply := range readReplies {
		if reply.Version > maxVersion {
			maxVersion = reply.Version
		}
	}

	// Increment version number for the write
	newVersion := maxVersion + 1

	// Phase 2: Write to quorum
	err = c.writeToQuorum(newVersion, value)
	if err != nil {
		return err
	}

	// Update LocalVersion
	c.LocalVersion = newVersion

	return nil
}

// readFromQuorum reads data from a quorum of servers.
// Returns a list of read replies or an error if the quorum is not achieved.
func (c *Client) readFromQuorum() ([]server.ReadReply, error) {
	quorumSize := (len(c.Servers) / 2) + 1
	var replies []server.ReadReply
	var mu sync.Mutex
	var wg sync.WaitGroup
	var acks int

	for _, serverConn := range c.Servers {
		wg.Add(1)
		go func(conn *protocol.Connection) {
			defer wg.Done()
			var reply server.ReadReply
			client, err := rpc.Dial(conn.Network, conn.Address)
			if err != nil {
				return
			}
			defer client.Close()
			err = client.Call("Server.HandleReadRequest", &server.ReadRequest{}, &reply)
			if err != nil {
				return
			}
			mu.Lock()
			replies = append(replies, reply)
			acks++
			mu.Unlock()
		}(serverConn)
	}

	wg.Wait()

	if acks < quorumSize {
		return nil, fmt.Errorf("read quorum not achieved: acks=%d, quorum=%d", acks, quorumSize)
	}

	return replies, nil
}

// writeToQuorum writes the given version and value to a quorum of servers.
// Returns an error if the quorum is not achieved.
func (c *Client) writeToQuorum(version uint64, value uint64) error {
	quorumSize := (len(c.Servers) / 2) + 1
	var acks int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, serverConn := range c.Servers {
		wg.Add(1)
		go func(conn *protocol.Connection) {
			defer wg.Done()
			var reply server.WriteReply
			writeReq := server.WriteRequest{
				Version: version,
				Value:   value,
			}
			client, err := rpc.Dial(conn.Network, conn.Address)
			if err != nil {
				return
			}
			defer client.Close()
			err = client.Call("Server.HandleWriteRequest", &writeReq, &reply)
			if err != nil {
				return
			}
			mu.Lock()
			acks++
			mu.Unlock()
		}(serverConn)
	}

	wg.Wait()

	if acks < quorumSize {
		return fmt.Errorf("write quorum not achieved: acks=%d, quorum=%d", acks, quorumSize)
	}

	return nil
}

// Start begins the client's operation loop, executing a series of instructions or workloads.
func (c *Client) Start(workload []workload.Instruction) error {
	log.Debugf("starting client %d", c.Id)

	// Metrics collection
	var totalOps, successfulOps, failedOps uint64
	var totalLatency time.Duration

	for i, instr := range workload {
		start := time.Now()

		// Execute instruction based on its type
		switch instr.Type {
		case "read":
			_, err := c.ABDRead()
			if err != nil {
				log.Errorf("Client %d failed to read: %v", c.Id, err)
				failedOps++
			} else {
				log.Infof("Client %d successfully read", c.Id)
				successfulOps++
			}

		case "write":
			err := c.ABDWrite(instr.Value)
			if err != nil {
				log.Errorf("Client %d failed to write value %d: %v", c.Id, instr.Value, err)
				failedOps++
			} else {
				log.Infof("Client %d successfully wrote value %d", c.Id, instr.Value)
				successfulOps++
			}

		default:
			log.Warnf("Unknown instruction type: %s", instr.Type)
			continue
		}

		// Track latency
		elapsed := time.Since(start)
		totalLatency += elapsed

		// Increment total operations
		totalOps++

		// Optional: Sleep between operations if specified in the workload
		if instr.Delay > 0 {
			time.Sleep(instr.Delay)
		}

		log.Infof("Completed instruction %d/%d: %v", i+1, len(workload), instr)
	}

	// Print metrics at the end
	log.Infof("Client %d completed workload. Total Ops: %d, Success: %d, Failed: %d, Avg Latency: %v",
		c.Id, totalOps, successfulOps, failedOps, totalLatency/time.Duration(totalOps))

	return nil
}
