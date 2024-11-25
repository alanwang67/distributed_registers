package server

import (
	"fmt"
	"sort"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/vectorclock"
)

// New creates and initializes a new Server instance with the given ID, self connection, and peer connections.
func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	s := &Server{
		Id:                  id,
		Self:                self,
		Peers:               peers,
		VectorClock:         make([]uint64, len(peers)),
		MyOperations:        make([]Operation, 0),
		OperationsPerformed: make([]Operation, 0),
		PendingOperations:   make([]Operation, 0),
		Data:                0,
	}
	go s.sendGossip()
	return s
}

// DependencyCheck verifies if the server's vector clock satisfies the client's dependency
// requirements based on the session type.
func DependencyCheck(serverData Server, request ClientRequest) bool {
	switch request.SessionType {
	case Causal:
		return vectorclock.CompareVersionVector(serverData.VectorClock, request.WriteVector) &&
			vectorclock.CompareVersionVector(serverData.VectorClock, request.ReadVector)
	case MonotonicReads:
		return vectorclock.CompareVersionVector(serverData.VectorClock, request.WriteVector)
	case MonotonicWrites:
		return vectorclock.CompareVersionVector(serverData.VectorClock, request.WriteVector)
	case ReadYourWrites:
		return vectorclock.CompareVersionVector(serverData.VectorClock, request.ReadVector)
	case WritesFollowReads:
		return vectorclock.CompareVersionVector(serverData.VectorClock, request.ReadVector)
	default:
		panic("Unspecified session type")
	}
}

// operationsGetMaxVersionVector computes the maximum version vector from a list of operations.
// It returns a new version vector where each element is the maximum across all operations.
func operationsGetMaxVersionVector(lst []Operation) []uint64 {
	if len(lst) == 0 {
		return nil
	}
	// Initialize mx as a copy of the first operation's VersionVector
	mx := make([]uint64, len(lst[0].VersionVector))
	copy(mx, lst[0].VersionVector)
	for i := 1; i < len(lst); i++ {
		for j := 0; j < len(lst[i].VersionVector); j++ {
			if lst[i].VersionVector[j] > mx[j] {
				mx[j] = lst[i].VersionVector[j]
			}
		}
	}
	return mx
}

// ProcessClientRequest processes a client's read or write request and populates the reply accordingly.
func (s *Server) ProcessClientRequest(request *ClientRequest, reply *ClientReply) error {
	if !(DependencyCheck(*s, *request)) {
		reply.Succeeded = false
		return nil
	}

	if request.OperationType == Read {
		if len(s.OperationsPerformed) == 0 {
			reply.Succeeded = true
			reply.OperationType = Read
			reply.Data = s.Data
			reply.ReadVector = request.ReadVector
			reply.WriteVector = request.WriteVector
		}

		reply.Succeeded = true
		reply.OperationType = Read
		reply.Data = s.Data

		// Update the client's read vector with the maximum of its current read vector and the server's vector clock
		reply.ReadVector = vectorclock.GetMaxVersionVector(append([][]uint64{request.ReadVector}, append([]uint64(nil), s.VectorClock...)))
		reply.WriteVector = request.WriteVector
		return nil
	} else {
		s.VectorClock[s.Id] += 1

		s.OperationsPerformed = append(
			s.OperationsPerformed,
			Operation{
				OperationType: Write,
				VersionVector: append([]uint64(nil), s.VectorClock...),
				TieBreaker:    s.Id,
				Data:          request.Data,
			})
		s.MyOperations = append(
			s.MyOperations,
			Operation{
				OperationType: Write,
				VersionVector: append([]uint64(nil), s.VectorClock...),
				TieBreaker:    s.Id,
				Data:          request.Data,
			})

		fmt.Print(s.MyOperations)
		s.Data = request.Data
		reply.Succeeded = true
		reply.OperationType = Write
		reply.Data = request.Data
		reply.ReadVector = request.ReadVector
		reply.WriteVector = append([]uint64(nil), s.VectorClock...)
		return nil
	}
}

// oneOff checks if o2 is directly dependent on o1, i.e., if o2's vector clock is exactly one increment ahead
func oneOffVersionVector(serverId uint64, v1 []uint64, v2 []uint64) bool {
	ct := 0

	for i := 0; i < len(v1); i++ {
		if i == int(serverId) {
			continue
		}
		if v1[i]+1 == v2[i] {
			ct += 1
		}
		if v1[i] == v2[i]+1 {
			ct += 1
		}
	}

	return ct == 1
}

// compareOperations compares two operations to determine their ordering.
// If the operations are concurrent, the tie-breaker (server ID) is used.
func compareOperations(o1 Operation, o2 Operation) bool {
	if vectorclock.ConcurrentVersionVectors(o1.VersionVector, o2.VersionVector) {
		return o1.TieBreaker > o2.TieBreaker
	}
	return vectorclock.CompareVersionVector(o1.VersionVector, o2.VersionVector)
}

// merge combines two lists of operations and sorts them using compareOperations.
func mergePendingOperations(l1 []Operation, l2 []Operation) []Operation {
	output := append(l1, l2...)
	sort.Slice(output, func(i, j int) bool {
		return compareOperations(output[j], output[i])
	})
	return output
}

// ReceiveGossip processes incoming gossip messages from peers and updates the server's state.
func (s *Server) ReceiveGossip(request *GossipRequest, reply *GossipReply) error {

	if len(request.Operations) == 0 {
		return nil
	}

	s.PendingOperations = mergePendingOperations(request.Operations, s.PendingOperations)

	latestVersionVector := make([]uint64, len(s.Peers))
	if len(s.OperationsPerformed) != 0 {
		latestVersionVector = s.OperationsPerformed[len(s.OperationsPerformed)-1].VersionVector
	}

	i := 0
	for i < len(s.PendingOperations) {
		// perform operation if it doesn't have any dependencies and remove it from the pending operations
		if vectorclock.CompareVersionVector(latestVersionVector, s.PendingOperations[i].VersionVector) {
			i += 1
		} else if oneOffVersionVector(s.Id, latestVersionVector, s.PendingOperations[i].VersionVector) {
			s.OperationsPerformed = append(s.OperationsPerformed, s.PendingOperations[i])
			latestVersionVector = operationsGetMaxVersionVector(s.OperationsPerformed)
			i += 1
		} else {
			break
		}
	}
	s.PendingOperations = s.PendingOperations[i:]

	sort.Slice(s.OperationsPerformed, func(i, j int) bool {
		return compareOperations(s.OperationsPerformed[j], s.OperationsPerformed[i])
	})

	if len(s.OperationsPerformed) != 0 {
		s.Data = s.OperationsPerformed[len(s.OperationsPerformed)-1].Data
		s.VectorClock = operationsGetMaxVersionVector(s.OperationsPerformed)
	}

	return nil
}

// sendGossip sends the server's operations to all peers to synchronize state.
func (s *Server) sendGossip() {
	for {
		ms := 1000
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if len(s.MyOperations) == 0 {
			continue
		}

		for i := range s.Peers {
			if i != int(s.Id) {
				req := &GossipRequest{ServerId: s.Id, Operations: s.MyOperations}
				reply := &GossipReply{}
				protocol.Invoke(*s.Peers[i], "Server.ReceiveGossip", &req, &reply)
			}
		}
	}
}
