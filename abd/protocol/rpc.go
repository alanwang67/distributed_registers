package protocol

import (
	"net/rpc"
	"time"
)

// Heartbeat represents the periodic state message sent to peers.
type Heartbeat struct {
	Version uint64
	Value   uint64
}

// HeartbeatReply represents the response to a heartbeat message.
type HeartbeatReply struct {
	Acknowledged bool
}

// ReadRequest represents a request for reading the register state.
type ReadRequest struct{}

// ReadReply represents the response to a read request.
type ReadReply struct {
	Version uint64 // The version of the register
	Value   uint64 // The value of the register
}

// WriteRequest represents a request to update the register state.
type WriteRequest struct {
	Version uint64 // The version to be written
	Value   uint64 // The value to be written
}

// WriteReply represents the response to a write request.
type WriteReply struct{}

// ReadConfirmRequest represents a request to confirm a read value to servers.
type ReadConfirmRequest struct {
	Version uint64 // The version confirmed
	Value   uint64 // The value confirmed
}

// ReadConfirmReply represents the response to a read confirm request.
type ReadConfirmReply struct {
	Acknowledged bool
}

// Connection represents a server connection.
type Connection struct {
	Network string // Network type (e.g., "tcp")
	Address string // Address of the server (e.g., "192.168.1.1:10000")
}

// Invoke handles RPC calls to the server.
func Invoke(conn Connection, method string, request interface{}, reply interface{}) error {
	client, err := rpc.Dial(conn.Network, conn.Address)
	if err != nil {
		return err
	}
	defer client.Close()

	// Perform the RPC call
	call := client.Go(method, request, reply, nil)

	// Wait for the response with a timeout
	select {
	case <-call.Done:
		return call.Error
	case <-time.After(5 * time.Second):
		return rpc.ErrShutdown
	}
}
