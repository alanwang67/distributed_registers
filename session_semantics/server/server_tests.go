package server_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/alanwang67/distributed_registers/server"
	"github.com/alanwang67/distributed_registers/vectorclock"
)

// Mock SessionType enumeration
type SessionType int

const (
	Causal SessionType = iota
	MonotonicReads
	MonotonicWrites
	ReadYourWrites
	WritesFollowReads
)

// Mock OperationType enumeration
type OperationType int

const (
	Read OperationType = iota
	Write
)

// Mock Operation struct
type Operation struct {
	OperationType OperationType
	VersionVector []uint64
	TieBreaker    uint64
	Data          int
}

// Mock ClientRequest struct
type ClientRequest struct {
	OperationType OperationType
	SessionType   SessionType
	ReadVector    []uint64
	WriteVector   []uint64
	Data          int
}

// Mock ClientReply struct
type ClientReply struct {
	Succeeded     bool
	OperationType OperationType
	Data          int
	ReadVector    []uint64
	WriteVector   []uint64
}

// Mock GossipRequest and GossipReply
type GossipRequest struct {
	ServerId   uint64
	Operations []Operation
}

type GossipReply struct{}

// Mock protocol package
package protocol

type Connection struct{}

func Invoke(conn Connection, method string, req interface{}, reply interface{}) error {
	// Mock implementation
	return nil
}

// Test cases start here

func TestNew(t *testing.T) {
	selfConn := &protocol.Connection{}
	peerConns := []*protocol.Connection{
		&protocol.Connection{},
		&protocol.Connection{},
	}
	id := uint64(1)

	s := server.New(id, selfConn, peerConns)

	if s.Id != id {
		t.Errorf("Expected server ID %d, got %d", id, s.Id)
	}
	if s.Self != selfConn {
		t.Errorf("Expected self connection to be %v, got %v", selfConn, s.Self)
	}
	if !reflect.DeepEqual(s.Peers, peerConns) {
		t.Errorf("Expected peers %v, got %v", peerConns, s.Peers)
	}
	expectedVectorClock := make([]uint64, len(peerConns))
	if !reflect.DeepEqual(s.VectorClock, expectedVectorClock) {
		t.Errorf("Expected VectorClock %v, got %v", expectedVectorClock, s.VectorClock)
	}
	if len(s.OperationsPerformed) != 0 {
		t.Errorf("Expected OperationsPerformed to be empty, got %v", s.OperationsPerformed)
	}
	if len(s.PendingOperations) != 0 {
		t.Errorf("Expected PendingOperations to be empty, got %v", s.PendingOperations)
	}
	if s.Data != 0 {
		t.Errorf("Expected Data to be 0, got %d", s.Data)
	}
}

func TestDependencyCheck(t *testing.T) {
	serverData := server.Server{
		VectorClock: []uint64{2, 3, 5},
	}
	tests := []struct {
		name     string
		request  server.ClientRequest
		expected bool
	}{
		{
			name: "Causal satisfied",
			request: server.ClientRequest{
				SessionType: server.Causal,
				ReadVector:  []uint64{1, 2, 4},
				WriteVector: []uint64{2, 3, 5},
			},
			expected: true,
		},
		{
			name: "Causal not satisfied",
			request: server.ClientRequest{
				SessionType: server.Causal,
				ReadVector:  []uint64{3, 3, 5},
				WriteVector: []uint64{2, 4, 5},
			},
			expected: false,
		},
		{
			name: "MonotonicReads satisfied",
			request: server.ClientRequest{
				SessionType: server.MonotonicReads,
				ReadVector:  []uint64{0, 0, 0},
				WriteVector: []uint64{2, 3, 5},
			},
			expected: true,
		},
		{
			name: "MonotonicWrites not satisfied",
			request: server.ClientRequest{
				SessionType: server.MonotonicWrites,
				WriteVector: []uint64{3, 3, 6},
			},
			expected: false,
		},
		{
			name: "ReadYourWrites satisfied",
			request: server.ClientRequest{
				SessionType: server.ReadYourWrites,
				ReadVector:  []uint64{2, 3, 5},
			},
			expected: true,
		},
		{
			name: "WritesFollowReads not satisfied",
			request: server.ClientRequest{
				SessionType: server.WritesFollowReads,
				ReadVector:  []uint64{3, 3, 6},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		result := server.DependencyCheck(serverData, tt.request)
		if result != tt.expected {
			t.Errorf("%s: Expected %v, got %v", tt.name, tt.expected, result)
		}
	}
}

func TestOperationsGetMaxVersionVector(t *testing.T) {
	ops := []server.Operation{
		{VersionVector: []uint64{1, 2, 3}},
		{VersionVector: []uint64{2, 1, 4}},
		{VersionVector: []uint64{0, 3, 2}},
	}
	expected := []uint64{2, 3, 4}

	result := server.OperationsGetMaxVersionVector(ops)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}

	// Test with empty list
	result = server.OperationsGetMaxVersionVector([]server.Operation{})
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}