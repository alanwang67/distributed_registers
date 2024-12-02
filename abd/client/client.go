package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"github.com/alanwang67/distributed_registers/abd/workload"
	"github.com/charmbracelet/log"
)

// Client represents an ABD client that communicates with distributed servers.
type Client struct {
	Id           uint64                 // Unique ID of the client
	Servers      []*protocol.Connection // List of server connections
	LocalVersion uint64                 // Client's local version number for session guarantees
}

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
	var maxVersion uint64
	var value uint64
	for i, reply := range readReplies {
		if i == 0 || reply.Version > maxVersion {
			maxVersion = reply.Version
			value = reply.Value
		}
	}

	// Update LocalVersion for session guarantees
	if maxVersion > c.LocalVersion {
		c.LocalVersion = maxVersion
	}

	// Write-Back Phase
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
	var maxVersion uint64
	for _, reply := range readReplies {
		if reply.Version > maxVersion {
			maxVersion = reply.Version
		}
	}
	if maxVersion < c.LocalVersion {
		maxVersion = c.LocalVersion
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
	replies := make([]server.ReadReply, 0, len(c.Servers))
	var mu sync.Mutex
	errCh := make(chan error, len(c.Servers))
	doneCh := make(chan struct{})
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, serverConn := range c.Servers {
		wg.Add(1)
		go func(conn *protocol.Connection) {
			defer wg.Done()
			select {
			case <-doneCh:
				return
			default:
			}

			client, err := protocol.DialContext(ctx, conn.Network, conn.Address)
			if err != nil {
				errCh <- fmt.Errorf("dial error: %v", err)
				return
			}
			defer client.Close()
			var reply server.ReadReply
			err = client.Call("Server.HandleReadRequest", &server.ReadRequest{}, &reply)
			if err != nil {
				errCh <- fmt.Errorf("call error: %v", err)
				return
			}
			mu.Lock()
			replies = append(replies, reply)
			if len(replies) >= quorumSize {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			mu.Unlock()
		}(serverConn)
	}

	// Wait for all goroutines to finish or context timeout
	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
	}

	// Collect errors
	var lastErr error
	for err := range errCh {
		lastErr = err
		log.Errorf("readFromQuorum error: %v", err)
	}

	// Check if quorum is achieved
	mu.Lock()
	numReplies := len(replies)
	mu.Unlock()
	if numReplies < quorumSize {
		return nil, fmt.Errorf("read quorum not achieved: acks=%d, quorum=%d, last error: %v", numReplies, quorumSize, lastErr)
	}

	// Return only up to quorumSize replies
	return replies[:quorumSize], nil
}

// writeToQuorum writes the given version and value to a quorum of servers.
// Returns an error if the quorum is not achieved.
func (c *Client) writeToQuorum(version uint64, value uint64) error {
	quorumSize := (len(c.Servers) / 2) + 1
	var wg sync.WaitGroup
	ackCh := make(chan struct{}, len(c.Servers))
	errCh := make(chan error, len(c.Servers))
	doneCh := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, serverConn := range c.Servers {
		wg.Add(1)
		go func(conn *protocol.Connection) {
			defer wg.Done()
			select {
			case <-doneCh:
				return
			default:
			}

			client, err := protocol.DialContext(ctx, conn.Network, conn.Address)
			if err != nil {
				errCh <- fmt.Errorf("dial error: %v", err)
				return
			}
			defer client.Close()
			writeReq := server.WriteRequest{
				Version: version,
				Value:   value,
			}
			var reply server.WriteReply
			err = client.Call("Server.HandleWriteRequest", &writeReq, &reply)
			if err != nil {
				errCh <- fmt.Errorf("call error: %v", err)
				return
			}
			ackCh <- struct{}{}
			if len(ackCh) >= quorumSize {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		}(serverConn)
	}

	// Wait for all goroutines to finish or context timeout
	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
	}

	// Collect errors
	var lastErr error
	for err := range errCh {
		lastErr = err
		log.Errorf("writeToQuorum error: %v", err)
	}

	// Check if quorum is achieved
	acks := len(ackCh)
	if acks < quorumSize {
		return fmt.Errorf("write quorum not achieved: acks=%d, quorum=%d, last error: %v", acks, quorumSize, lastErr)
	}

	return nil
}

// Start begins the client's operation loop, executing a series of instructions.
func (c *Client) Start(instructions []workload.Instruction) error {
	log.Debugf("starting client %d", c.Id)

	// Metrics collection
	var totalOps, successfulOps, failedOps uint64
	var totalLatency time.Duration

	for i, instr := range instructions {
		start := time.Now()

		// Execute instruction based on its type
		switch instr.Type {
		case workload.InstructionTypeRead:
			value, err := c.ABDRead()
			if err != nil {
				log.Errorf("Client %d failed to read: %v", c.Id, err)
				failedOps++
			} else {
				log.Infof("Client %d successfully read value %d", c.Id, value)
				successfulOps++
			}

		case workload.InstructionTypeWrite:
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

		// Optional: Sleep between operations if specified in the instruction
		if instr.Delay > 0 {
			time.Sleep(instr.Delay)
		}

		log.Infof("Completed instruction %d/%d: %v", i+1, len(instructions), instr)
	}

	// Compute average latency
	var avgLatency time.Duration
	if totalOps > 0 {
		avgLatency = totalLatency / time.Duration(totalOps)
	} else {
		avgLatency = 0
	}

	// Print metrics at the end
	log.Infof("Client %d completed workload. Total Ops: %d, Success: %d, Failed: %d, Avg Latency: %v",
		c.Id, totalOps, successfulOps, failedOps, avgLatency)

	return nil
}
