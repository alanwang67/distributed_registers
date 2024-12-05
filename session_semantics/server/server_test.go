package server

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/vectorclock"
)

// TestNew tests the New function in the server package
func TestNew(t *testing.T) {
	id := uint64(0)
	self := &protocol.Connection{Network: "tcp", Address: "localhost:8080"}
	peers := []*protocol.Connection{
		{Network: "tcp", Address: "localhost:8081"},
		{Network: "tcp", Address: "localhost:8082"},
	}

	s := New(id, self, peers)

	if s.Id != id {
		t.Errorf("Expected server ID %d, got %d", id, s.Id)
	}
	if s.Self != self {
		t.Errorf("Expected self connection %v, got %v", self, s.Self)
	}
	if !reflect.DeepEqual(s.Peers, peers) {
		t.Errorf("Expected peers %v, got %v", peers, s.Peers)
	}
	expectedVectorClock := make([]uint64, len(peers))
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
	serverData := Server{
		Id:          0,
		VectorClock: []uint64{2, 3, 5},
	}

	tests := []struct {
		name     string
		request  ClientRequest
		expected bool
	}{
		{
			name: "Causal dependency satisfied",
			request: ClientRequest{
				SessionType: Causal,
				ReadVector:  []uint64{1, 2, 4},
				WriteVector: []uint64{2, 3, 5},
			},
			expected: true,
		},
		{
			name: "Causal dependency not satisfied",
			request: ClientRequest{
				SessionType: Causal,
				ReadVector:  []uint64{2, 3, 5},
				WriteVector: []uint64{2, 3, 6},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		result := DependencyCheck(serverData.VectorClock, tt.request) // Fixed here
		if result != tt.expected {
			t.Errorf("%s: Expected %v, got %v", tt.name, tt.expected, result)
		}
	}
}

func TestOperationsGetMaxVersionVector(t *testing.T) {
	ops := []Operation{
		{VersionVector: []uint64{1, 2, 3}},
		{VersionVector: []uint64{2, 1, 4}},
		{VersionVector: []uint64{0, 3, 2}},
	}
	expected := []uint64{2, 3, 4}

	result := operationsGetMaxVersionVector(ops)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}

	// Test with empty list
	result = operationsGetMaxVersionVector([]Operation{})
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestProcessClientRequest_Read(t *testing.T) {
	s := &Server{
		Id:          0,
		VectorClock: []uint64{1, 2, 3},
		Data:        42,
	}

	request := &ClientRequest{
		OperationType: Read,
		SessionType:   Causal,
		ReadVector:    []uint64{0, 0, 0},
		WriteVector:   []uint64{0, 0, 0},
	}

	reply := &ClientReply{}

	err := s.ProcessClientRequest(request, reply)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !reply.Succeeded {
		t.Errorf("Expected success, got failure")
	}
	if reply.Data != s.Data {
		t.Errorf("Expected data %d, got %d", s.Data, reply.Data)
	}
	// Check that ReadVector is updated correctly
	expectedReadVector := vectorclock.GetMaxVersionVector([][]uint64{request.ReadVector, s.VectorClock})
	if !reflect.DeepEqual(reply.ReadVector, expectedReadVector) {
		t.Errorf("Expected ReadVector %v, got %v", expectedReadVector, reply.ReadVector)
	}
}

func TestProcessClientRequest_Write(t *testing.T) {
	s := &Server{
		Id:          1,
		VectorClock: []uint64{1, 2, 3},
	}

	request := &ClientRequest{
		OperationType: Write,
		SessionType:   MonotonicWrites,
		ReadVector:    []uint64{1, 2, 3},
		WriteVector:   []uint64{1, 2, 3},
		Data:          99,
	}

	reply := &ClientReply{}

	err := s.ProcessClientRequest(request, reply)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !reply.Succeeded {
		t.Errorf("Expected success, got failure")
	}
	if reply.Data != request.Data {
		t.Errorf("Expected data %d, got %d", request.Data, reply.Data)
	}
	if reply.OperationType != Write {
		t.Errorf("Expected OperationType Write, got %v", reply.OperationType)
	}
	// Check that VectorClock is incremented
	expectedVectorClock := []uint64{1, 3, 3}
	if !reflect.DeepEqual(s.VectorClock, expectedVectorClock) {
		t.Errorf("Expected VectorClock %v, got %v", expectedVectorClock, s.VectorClock)
	}
	// Check that OperationsPerformed is updated
	if len(s.OperationsPerformed) != 1 {
		t.Errorf("Expected 1 operation performed, got %d", len(s.OperationsPerformed))
	}
}

func TestOneOffVersionVector(t *testing.T) {
	serverId := uint64(0)
	v1 := []uint64{1, 2, 3}
	v2 := []uint64{1, 3, 3} // Difference at index 1, so true
	v3 := []uint64{2, 2, 3} // Difference at index 0 (serverId), so false
	v4 := []uint64{1, 2, 4} // Difference at index 2, so true

	tests := []struct {
		v1       []uint64
		v2       []uint64
		expected bool
	}{
		{v1: v1, v2: v2, expected: true},                 // Increment at index 1
		{v1: v1, v2: v3, expected: false},                // Increment at index 0 (serverId, ignored)
		{v1: v1, v2: v4, expected: true},                 // Increment at index 2
		{v1: v1, v2: v1, expected: false},                // No differences
		{v1: v1, v2: []uint64{2, 3, 4}, expected: false}, // Differences at multiple indices
	}

	for _, tt := range tests {
		result := oneOffVersionVector(serverId, tt.v1, tt.v2)
		if result != tt.expected {
			t.Errorf("oneOffVersionVector(%v, %v): expected %v, got %v", tt.v1, tt.v2, tt.expected, result)
		}
	}
}

func TestCompareOperations(t *testing.T) {
	tests := []struct {
		o1     Operation
		o2     Operation
		expect bool
	}{
		// Identical version vectors, o1.TieBreaker < o2.TieBreaker
		{
			o1:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 1},
			o2:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 2},
			expect: false,
		},
		// Identical version vectors, o1.TieBreaker > o2.TieBreaker
		{
			o1:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 2},
			o2:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 1},
			expect: true,
		},
		// Non-concurrent version vectors, o1 > o2
		{
			o1:     Operation{VersionVector: []uint64{2, 3, 4}, TieBreaker: 0},
			o2:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 0},
			expect: true,
		},
		// Non-concurrent version vectors, o1 < o2
		{
			o1:     Operation{VersionVector: []uint64{1, 2, 3}, TieBreaker: 0},
			o2:     Operation{VersionVector: []uint64{2, 3, 4}, TieBreaker: 0},
			expect: false,
		},
		// Concurrent version vectors, o1.TieBreaker > o2.TieBreaker
		{
			o1:     Operation{VersionVector: []uint64{1, 3, 2}, TieBreaker: 5},
			o2:     Operation{VersionVector: []uint64{2, 1, 3}, TieBreaker: 3},
			expect: true,
		},
		// Concurrent version vectors, o1.TieBreaker < o2.TieBreaker
		{
			o1:     Operation{VersionVector: []uint64{1, 3, 2}, TieBreaker: 1},
			o2:     Operation{VersionVector: []uint64{2, 1, 3}, TieBreaker: 5},
			expect: false,
		},
	}

	for _, tt := range tests {
		result := compareOperations(tt.o1, tt.o2)
		if result != tt.expect {
			t.Errorf("compareOperations(%v, %v) = %v; want %v", tt.o1, tt.o2, result, tt.expect)
		}
	}
}

func TestMergePendingOperations(t *testing.T) {
	op1 := Operation{
		VersionVector: []uint64{1, 2, 3},
		TieBreaker:    1,
	}
	op2 := Operation{
		VersionVector: []uint64{2, 2, 3},
		TieBreaker:    0,
	}
	op3 := Operation{
		VersionVector: []uint64{1, 3, 3},
		TieBreaker:    2,
	}
	l1 := []Operation{op1}
	l2 := []Operation{op2, op3}

	merged := mergePendingOperations(l1, l2)
	expected := []Operation{op3, op2, op1}

	if !reflect.DeepEqual(merged, expected) {
		t.Errorf("Expected merged list %v, got %v", expected, merged)
	}
}

func TestReceiveGossip(t *testing.T) {
	s := &Server{
		Id:          0,
		VectorClock: []uint64{1, 2, 3},
		OperationsPerformed: []Operation{
			{
				VersionVector: []uint64{1, 2, 3},
				TieBreaker:    0,
				Data:          42,
			},
		},
	}

	// Incoming gossip operations
	gossipOps := []Operation{
		{
			VersionVector: []uint64{1, 3, 3},
			TieBreaker:    1,
			Data:          43,
		},
		{
			VersionVector: []uint64{2, 2, 3},
			TieBreaker:    2,
			Data:          44,
		},
	}

	request := &GossipRequest{
		ServerId:   1,
		Operations: gossipOps,
	}
	reply := &GossipReply{}

	err := s.ReceiveGossip(request, reply)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Expected operations after merging and processing
	expectedOperations := append(s.OperationsPerformed, gossipOps...)
	sort.Slice(expectedOperations, func(i, j int) bool {
		return compareOperations(expectedOperations[j], expectedOperations[i])
	})

	if !reflect.DeepEqual(s.OperationsPerformed, expectedOperations) {
		t.Errorf("Expected OperationsPerformed %v, got %v", expectedOperations, s.OperationsPerformed)
	}

	// Check if VectorClock is updated
	expectedVectorClock := operationsGetMaxVersionVector(s.OperationsPerformed)
	if !reflect.DeepEqual(s.VectorClock, expectedVectorClock) {
		t.Errorf("Expected VectorClock %v, got %v", expectedVectorClock, s.VectorClock)
	}

	// Check if Data is updated to the latest operation's data
	expectedData := s.OperationsPerformed[len(s.OperationsPerformed)-1].Data
	if s.Data != expectedData {
		t.Errorf("Expected Data %d, got %d", expectedData, s.Data)
	}
}

func TestGossipProtocolIntegration(t *testing.T) {
	// Create mock connections
	connections := []*protocol.Connection{
		{Network: "tcp", Address: "localhost:9001"},
		{Network: "tcp", Address: "localhost:9002"},
		{Network: "tcp", Address: "localhost:9003"},
	}

	// Create servers
	servers := []*Server{
		New(0, connections[0], connections[1:]),
		New(1, connections[1], []*protocol.Connection{connections[0], connections[2]}),
		New(2, connections[2], connections[:2]),
	}

	// Start servers (in separate goroutines, simulating real servers)
	errorChan := make(chan error, len(servers))

	for _, s := range servers {
		go func(srv *Server) {
			err := srv.Start()
			errorChan <- err // Send error (or nil) to channel
		}(s)
	}

	// Collect errors
	for i := 0; i < len(servers); i++ {
		err := <-errorChan
		if err != nil {
			t.Fatalf("Failed to start server: %v", err)
		}
	}

	// Allow servers to start up
	time.Sleep(2 * time.Second)

	// Simulate operations on servers
	clientRequest1 := &ClientRequest{
		OperationType: Write,
		SessionType:   Causal,
		ReadVector:    []uint64{0, 0, 0},
		WriteVector:   []uint64{0, 0, 0},
		Data:          100,
	}

	clientReply1 := &ClientReply{}
	err := servers[0].ProcessClientRequest(clientRequest1, clientReply1)
	if err != nil || !clientReply1.Succeeded {
		t.Fatalf("Server 0 failed to process write request: %v", err)
	}

	clientRequest2 := &ClientRequest{
		OperationType: Write,
		SessionType:   Causal,
		ReadVector:    []uint64{0, 0, 0},
		WriteVector:   []uint64{0, 0, 0},
		Data:          200,
	}

	clientReply2 := &ClientReply{}
	err = servers[1].ProcessClientRequest(clientRequest2, clientReply2)
	if err != nil || !clientReply2.Succeeded {
		t.Fatalf("Server 1 failed to process write request: %v", err)
	}

	// Allow gossip to propagate
	time.Sleep(5 * time.Second)

	// Verify that all servers converge to the same state
	expectedData := uint64(200)
	expectedVectorClock := []uint64{1, 1, 0}

	for i, srv := range servers {
		if srv.Data != expectedData {
			t.Errorf("Server %d has incorrect Data: got %d, expected %d", i, srv.Data, expectedData)
		}

		if !reflect.DeepEqual(srv.VectorClock, expectedVectorClock) {
			t.Errorf("Server %d has incorrect VectorClock: got %v, expected %v", i, srv.VectorClock, expectedVectorClock)
		}
	}

	// Optionally: Shut down servers (cleanup)
}

func TestEventualConvergence(t *testing.T) {
	// Simulate two servers that exchange operations via gossip
	server1 := New(0, &protocol.Connection{}, []*protocol.Connection{})
	server2 := New(1, &protocol.Connection{}, []*protocol.Connection{})

	// Initial vector clocks
	server1.VectorClock = []uint64{1, 0}
	server2.VectorClock = []uint64{0, 1}

	// Server1 performs a write operation
	clientRequest1 := &ClientRequest{
		OperationType: Write,
		SessionType:   Causal,
		ReadVector:    []uint64{0, 0},
		WriteVector:   []uint64{0, 0},
		Data:          100,
	}

	clientReply1 := &ClientReply{}
	err := server1.ProcessClientRequest(clientRequest1, clientReply1)
	if err != nil || !clientReply1.Succeeded {
		t.Fatalf("Server1 failed to process write request: %v", err)
	}

	// Server2 performs a write operation
	clientRequest2 := &ClientRequest{
		OperationType: Write,
		SessionType:   Causal,
		ReadVector:    []uint64{0, 0},
		WriteVector:   []uint64{0, 0},
		Data:          200,
	}

	clientReply2 := &ClientReply{}
	err = server2.ProcessClientRequest(clientRequest2, clientReply2)
	if err != nil || !clientReply2.Succeeded {
		t.Fatalf("Server2 failed to process write request: %v", err)
	}

	// Exchange gossip messages between the servers
	gossipRequest1 := &GossipRequest{
		ServerId:   server1.Id,
		Operations: server1.OperationsPerformed,
	}

	gossipReply1 := &GossipReply{}
	err = server2.ReceiveGossip(gossipRequest1, gossipReply1)
	if err != nil {
		t.Fatalf("Server2 failed to receive gossip from Server1: %v", err)
	}

	gossipRequest2 := &GossipRequest{
		ServerId:   server2.Id,
		Operations: server2.OperationsPerformed,
	}

	gossipReply2 := &GossipReply{}
	err = server1.ReceiveGossip(gossipRequest2, gossipReply2)
	if err != nil {
		t.Fatalf("Server1 failed to receive gossip from Server2: %v", err)
	}

	// Servers should eventually have the same data and vector clocks
	if !reflect.DeepEqual(server1.VectorClock, server2.VectorClock) {
		t.Errorf("Servers have different vector clocks: Server1 %v, Server2 %v", server1.VectorClock, server2.VectorClock)
	}

	if server1.Data != server2.Data {
		t.Errorf("Servers have different data values: Server1 %d, Server2 %d", server1.Data, server2.Data)
	}

	// Optionally, check that the vector clock is the element-wise maximum
	expectedVectorClock := vectorclock.GetMaxVersionVector([][]uint64{server1.VectorClock, server2.VectorClock})

	if !reflect.DeepEqual(server1.VectorClock, expectedVectorClock) {
		t.Errorf("Server1 VectorClock does not match expected: got %v, expected %v", server1.VectorClock, expectedVectorClock)
	}

	if !reflect.DeepEqual(server2.VectorClock, expectedVectorClock) {
		t.Errorf("Server2 VectorClock does not match expected: got %v, expected %v", server2.VectorClock, expectedVectorClock)
	}
}
