package client

import (
	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
)

// Client represents a client in the distributed register system.
type Client struct {
	Id          uint64                 // Unique identifier for the client
	Servers     []*protocol.Connection // List of server connections
	ReadVector  []uint64               // Client's current read vector clock
	WriteVector []uint64               // Client's current write vector clock
}
