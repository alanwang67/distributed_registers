package server

import (
	"sync"

	"github.com/alanwang67/distributed_registers/abd/protocol"
)

// Server represents a server in the ABD protocol

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	mutex   sync.Mutex
	Version uint64 // Version number for the server
	Value   uint64 // Value stored in the server
}

type ReadRequest struct{}

type ReadReply struct {
	Version uint64
	Value   uint64
}

type WriteRequest struct {
	Version uint64
	Value   uint64
}

type WriteReply struct{}
