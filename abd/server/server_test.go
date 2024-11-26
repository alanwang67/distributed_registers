package server

import (
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/stretchr/testify/assert"
)

func TestServerInitialization(t *testing.T) {
	id := uint64(0)
	self := &protocol.Connection{
		Network: "tcp",
		Address: "127.0.0.1:10000",
	}
	peers := []*protocol.Connection{}

	s := New(id, self, peers)

	assert.Equal(t, uint64(0), s.Version, "Initial version should be 0")
	assert.Equal(t, uint64(0), s.Value, "Initial value should be 0")
}

func TestHandleReadRequest(t *testing.T) {
	s := setupTestServer()

	req := &ReadRequest{}
	reply := &ReadReply{}

	err := s.HandleReadRequest(req, reply)
	assert.NoError(t, err, "HandleReadRequest should not return an error")
	assert.Equal(t, s.Version, reply.Version, "Reply version should match server's version")
	assert.Equal(t, s.Value, reply.Value, "Reply value should match server's value")
}

func TestHandleWriteRequestHigherVersion(t *testing.T) {
	s := setupTestServer()

	req := &WriteRequest{
		Version: 1,
		Value:   42,
	}
	reply := &WriteReply{}

	err := s.HandleWriteRequest(req, reply)
	assert.NoError(t, err, "HandleWriteRequest should not return an error")
	assert.Equal(t, req.Version, s.Version, "Server version should be updated")
	assert.Equal(t, req.Value, s.Value, "Server value should be updated")
}

func TestHandleWriteRequestLowerVersion(t *testing.T) {
	s := setupTestServer()
	// Set server's version and value to higher numbers
	s.Version = 2
	s.Value = 100

	req := &WriteRequest{
		Version: 1, // Lower version
		Value:   42,
	}
	reply := &WriteReply{}

	err := s.HandleWriteRequest(req, reply)
	assert.NoError(t, err, "HandleWriteRequest should not return an error")
	assert.Equal(t, uint64(2), s.Version, "Server version should not be updated")
	assert.Equal(t, uint64(100), s.Value, "Server value should not be updated")
}

func TestConcurrentWriteRequests(t *testing.T) {
	s := setupTestServer()

	var wg sync.WaitGroup
	writeRequests := []WriteRequest{
		{Version: 1, Value: 10},
		{Version: 2, Value: 20},
		{Version: 3, Value: 30},
	}

	for _, req := range writeRequests {
		wg.Add(1)
		go func(r WriteRequest) {
			defer wg.Done()
			reply := &WriteReply{}
			err := s.HandleWriteRequest(&r, reply)
			assert.NoError(t, err, "HandleWriteRequest should not return an error")
		}(req)
	}

	wg.Wait()

	assert.Equal(t, uint64(3), s.Version, "Server version should be the highest version received")
	assert.Equal(t, uint64(30), s.Value, "Server value should be from the highest version write")
}

func TestConcurrentReadAndWriteRequests(t *testing.T) {
	s := setupTestServer()
	s.Version = 1
	s.Value = 100

	var wg sync.WaitGroup
	readReplies := make([]ReadReply, 5)

	// Start concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			req := &ReadRequest{}
			reply := &ReadReply{}
			err := s.HandleReadRequest(req, reply)
			assert.NoError(t, err, "HandleReadRequest should not return an error")
			readReplies[index] = *reply
		}(i)
	}

	// Start concurrent writes
	writeRequests := []WriteRequest{
		{Version: 2, Value: 200},
		{Version: 3, Value: 300},
	}

	for _, req := range writeRequests {
		wg.Add(1)
		go func(r WriteRequest) {
			defer wg.Done()
			reply := &WriteReply{}
			err := s.HandleWriteRequest(&r, reply)
			assert.NoError(t, err, "HandleWriteRequest should not return an error")
		}(req)
	}

	wg.Wait()

	// Verify that reads were consistent
	for _, reply := range readReplies {
		assert.GreaterOrEqual(t, s.Version, reply.Version, "Read reply version should not exceed server version")
	}

	assert.Equal(t, uint64(3), s.Version, "Server version should be the highest version received")
	assert.Equal(t, uint64(300), s.Value, "Server value should be from the highest version write")
}

func TestIgnoreOutdatedWrites(t *testing.T) {
	s := setupTestServer()
	s.Version = 50
	s.Value = 500

	var wg sync.WaitGroup
	writeRequests := []WriteRequest{
		{Version: 40, Value: 400}, // Outdated
		{Version: 60, Value: 600}, // Newer
		{Version: 45, Value: 450}, // Outdated
	}

	for _, req := range writeRequests {
		wg.Add(1)
		go func(r WriteRequest) {
			defer wg.Done()
			reply := &WriteReply{}
			err := s.HandleWriteRequest(&r, reply)
			assert.NoError(t, err, "HandleWriteRequest should not return an error")
		}(req)
	}

	wg.Wait()

	assert.Equal(t, uint64(60), s.Version, "Server version should be updated to the highest version")
	assert.Equal(t, uint64(600), s.Value, "Server value should be from the write with highest version")
}

func TestServerStart(t *testing.T) {
	id := uint64(0)
	self := &protocol.Connection{
		Network: "tcp",
		Address: "127.0.0.1:10000",
	}
	peers := []*protocol.Connection{}

	s := New(id, self, peers)

	go func() {
		err := s.Start()
		assert.NoError(t, err, "Server Start should not return an error")
	}()

	// Allow time for the server to start
	time.Sleep(100 * time.Millisecond)

	// Test if RPC methods are accessible
	client, err := rpc.Dial(self.Network, self.Address)
	assert.NoError(t, err, "Should be able to dial the server")

	var readReply ReadReply
	err = client.Call("Server.HandleReadRequest", &ReadRequest{}, &readReply)
	assert.NoError(t, err, "RPC call to HandleReadRequest should not return an error")
}

func setupTestServer() *Server {
	id := uint64(0)
	self := &protocol.Connection{
		Network: "tcp",
		Address: "127.0.0.1:0", // Use port 0 for a free port
	}
	peers := []*protocol.Connection{}

	s := New(id, self, peers)
	return s
}
