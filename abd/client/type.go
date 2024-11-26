package client

import (
	"github.com/alanwang67/distributed_registers/abd/protocol"
)

// Client represents an ABD client that communicates with distributed servers.
type Client struct {
	Id           uint64                 // Unique ID of the client
	Servers      []*protocol.Connection // List of server connections
	LocalVersion uint64                 // Client's local version number for session guarantees
}
