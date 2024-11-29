package client

import (
	"errors"
	"testing"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
	"github.com/stretchr/testify/assert"
)

// MockServer simulates a server for testing purposes.
type MockServer struct {
	Address        string
	Server         *server.Server
	ShouldRPCError bool
}

func NewMockServer() *MockServer {
	// Initialize a server with ID 0 and no peers for simplicity.
	srv := server.New(0, &protocol.Connection{}, []*protocol.Connection{})
	return &MockServer{
		Address: "mockServer",
		Server:  srv,
	}
}

// ProcessClientRequest simulates the server's RPC method.
func (ms *MockServer) ProcessClientRequest(req *server.ClientRequest, reply *server.ClientReply) error {
	if ms.ShouldRPCError {
		return errors.New("RPC error")
	}
	return ms.Server.ProcessClientRequest(req, reply)
}

// Invoke simulates the protocol.Invoke function for testing.
func (ms *MockServer) Invoke(method string, req interface{}, reply interface{}) error {
	if method != "Server.ProcessClientRequest" {
		return errors.New("unknown method")
	}
	return ms.ProcessClientRequest(req.(*server.ClientRequest), reply.(*server.ClientReply))
}

// TestClientWriteSuccess verifies that the client can successfully write to the server.
func TestClientWriteSuccess(t *testing.T) {
	mockServer := NewMockServer()
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
			// For testing, we can leave network details empty.
		},
	}
	client := New(1, servers)
	// Override protocol.Invoke to use mock server
	protocol.Invoke = mockServer.Invoke

	valueToWrite := uint64(42)
	success := client.writeValueToServer(valueToWrite)

	assert.True(t, success, "Client should successfully write to the server")
	// The client's WriteVector should be updated based on the server's vector clock.
	expectedVector := append([]uint64(nil), mockServer.Server.VectorClock...)
	assert.Equal(t, expectedVector, client.WriteVector, "Client's WriteVector should match server's vector clock")
}

// TestClientReadSuccess verifies that the client can successfully read from the server.
func TestClientReadSuccess(t *testing.T) {
	mockServer := NewMockServer()
	// Pre-populate the server with data.
	mockServer.Server.Data = 42
	mockServer.Server.VectorClock[0] = 1
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
		},
	}
	client := New(1, servers)
	protocol.Invoke = mockServer.Invoke

	value := client.readValueFromServer()

	assert.Equal(t, uint64(42), value, "Client should read the correct value from the server")
	expectedVector := append([]uint64(nil), mockServer.Server.VectorClock...)
	assert.Equal(t, expectedVector, client.ReadVector, "Client's ReadVector should match server's vector clock")
}

// TestClientWriteDepFailure simulates a dependency check failure during write.
func TestClientWriteDepFailure(t *testing.T) {
	mockServer := NewMockServer()
	// Set the server's vector clock lower than client's WriteVector to trigger dependency failure.
	mockServer.Server.VectorClock[0] = 0
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
		},
	}
	client := New(1, servers)
	protocol.Invoke = mockServer.Invoke

	// Set client's WriteVector higher than server's VectorClock
	client.WriteVector[0] = 1

	valueToWrite := uint64(42)
	success := client.writeValueToServer(valueToWrite)

	assert.False(t, success, "Client write should fail due to dependency check failure")
}

// TestClientReadDepFailure simulates a dependency check failure during read.
func TestClientReadDepFailure(t *testing.T) {
	mockServer := NewMockServer()
	// Set the server's vector clock lower than client's ReadVector to trigger dependency failure.
	mockServer.Server.VectorClock[0] = 0
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
		},
	}
	client := New(1, servers)
	protocol.Invoke = mockServer.Invoke

	// Set client's ReadVector higher than server's VectorClock
	client.ReadVector[0] = 1

	value := client.readValueFromServer()

	assert.Equal(t, uint64(0), value, "Client read should return 0 due to dependency check failure")
}

// TestClientRPCError simulates an RPC error.
func TestClientRPCError(t *testing.T) {
	mockServer := NewMockServer()
	mockServer.ShouldRPCError = true
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
		},
	}
	client := New(1, servers)
	protocol.Invoke = mockServer.Invoke

	valueToWrite := uint64(42)
	success := client.writeValueToServer(valueToWrite)

	assert.False(t, success, "Client write should fail due to RPC error")
}

// TestClientNoAvailableServers tests client behavior when no servers are available.
func TestClientNoAvailableServers(t *testing.T) {
	servers := []*protocol.Connection{}
	client := New(1, servers)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic when no servers are available")
		}
	}()

	client.writeValueToServer(42)
}

// TestClientSessionVectors verifies that the client updates its session vectors correctly.
func TestClientSessionVectors(t *testing.T) {
	mockServer := NewMockServer()
	servers := []*protocol.Connection{
		{
			Address: "mockServer",
		},
	}
	client := New(1, servers)
	protocol.Invoke = mockServer.Invoke

	// Initial vectors should be zero
	assert.Equal(t, make([]uint64, len(servers)), client.ReadVector, "Initial ReadVector should be zero")
	assert.Equal(t, make([]uint64, len(servers)), client.WriteVector, "Initial WriteVector should be zero")

	// Perform write operation
	valueToWrite := uint64(100)
	client.writeValueToServer(valueToWrite)
	expectedWriteVector := append([]uint64(nil), mockServer.Server.VectorClock...)
	assert.Equal(t, expectedWriteVector, client.WriteVector, "WriteVector should be updated after write")

	// Perform read operation
	value := client.readValueFromServer()
	expectedReadVector := append([]uint64(nil), mockServer.Server.VectorClock...)
	assert.Equal(t, uint64(100), value, "Client should read the correct value")
	assert.Equal(t, expectedReadVector, client.ReadVector, "ReadVector should be updated after read")
}

// TestClientDifferentSessionTypes tests the client with different session types.
func TestClientDifferentSessionTypes(t *testing.T) {
	sessionTypes := []server.SessionType{
		server.Causal,
		server.MonotonicReads,
		server.MonotonicWrites,
		server.ReadYourWrites,
		server.WritesFollowReads,
	}

	for _, sessionType := range sessionTypes {
		t.Run(sessionType.String(), func(t *testing.T) {
			mockServer := NewMockServer()
			servers := []*protocol.Connection{
				{
					Address: "mockServer",
				},
			}
			client := New(1, servers)
			protocol.Invoke = mockServer.Invoke

			// Prepare client request
			clientReq := server.ClientRequest{
				OperationType: server.Write,
				SessionType:   sessionType,
				Data:          50,
				ReadVector:    client.ReadVector,
				WriteVector:   client.WriteVector,
			}
			clientReply := server.ClientReply{}
			err := protocol.Invoke(*servers[0], "Server.ProcessClientRequest", &clientReq, &clientReply)
			assert.Nil(t, err, "RPC call should not return an error")

			// Determine expected success based on dependency check
			depCheckPassed := server.DependencyCheck(*mockServer.Server, clientReq)
			assert.Equal(t, depCheckPassed, clientReply.Succeeded, "Operation success should match dependency check result")

			if clientReply.Succeeded {
				// Update client's WriteVector
				client.WriteVector = clientReply.WriteVector
				expectedVector := append([]uint64(nil), mockServer.Server.VectorClock...)
				assert.Equal(t, expectedVector, client.WriteVector, "Client's WriteVector should match server's vector clock")
			}
		})
	}
}
