package client

import (
	"sync"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
)

// WorkloadOperation defines the structure for a workload operation.
type WorkloadOperation struct {
	Type  string `json:"Type"`
	Value uint64 `json:"Value"`
	Delay int    `json:"Delay"` // Delay in milliseconds
}

// Config defines the structure of the configuration file.
type Config struct {
	Workloads []WorkloadOperation `json:"workloads"`
}

// Client represents a distributed client interacting with servers.
type Client struct {
	Id          uint64
	Servers     []*protocol.Connection
	ReadVector  []uint64
	WriteVector []uint64
	mu          sync.Mutex
}
