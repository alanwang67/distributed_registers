package server

import (
	"sort"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/alanwang67/distributed_registers/vectorclock"
)

// New creates and initializes a new Server instance with the given ID, self connection, and peer connections.
func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	return &Server{
		Id:                  id,
		Self:                self,
		Peers:               peers,
		VectorClock:         make([]uint64, len(peers)),
		OperationsPerformed: make([]Operation, 0),
		PendingOperations:   make([]Operation, 0),
		Data:                0,
	}
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
		reply.ReadVector = vectorclock.GetMaxVersionVector(append([][]uint64{request.ReadVector}, s.VectorClock))
		reply.WriteVector = request.WriteVector
		return nil
	} else {
		s.VectorClock[s.Id] += 1

		s.OperationsPerformed = append(
			s.OperationsPerformed,
			Operation{
				OperationType: Write,
				VersionVector: s.VectorClock,
				TieBreaker:    s.Id,
				Data:          request.Data,
			})
		s.MyOperations = append(
			s.MyOperations,
			Operation{
				OperationType: Write,
				VersionVector: s.VectorClock,
				TieBreaker:    s.Id,
				Data:          request.Data,
			})
		s.Data = request.Data
		reply.Succeeded = true
		reply.OperationType = Write
		reply.Data = request.Data
		reply.ReadVector = request.ReadVector
		reply.WriteVector = s.VectorClock
		return nil
	}
}

// func sameOperation(o1 Operation, o2 Operation) bool {
// 	for i := uint64(0); i < uint64(len(o1.VersionVector)); i++ {
// 		if o1.VersionVector[i] != o2.VersionVector[i] {
// 			return false
// 		}
// 	}
// 	return o1.TieBreaker == o2.TieBreaker
// }

// we are applying it to o1's server
// (0 7 0) (1 0 0) (1 8 0) (2 0 0) (2 8) (1 0 1)
// (0 0 0) (1 0 0) (1 0 1)
// (0 0 0) (1 0 0)

// oneOff checks if o2 is directly dependent on o1, i.e., if o2's vector clock is exactly one increment ahead
// of o1's vector clock in a single element, and the rest are equal.
func oneOff(o1 Operation, o2 Operation) bool {
	ct := 0

	for i := 0; i < len(o1.VersionVector); i++ {
		if i == int(o1.TieBreaker) {
			continue
		}
		if o1.VersionVector[i]+1 == o2.VersionVector[i] {
			ct += 1
		}
		if o1.VersionVector[i] == o2.VersionVector[i]+1 {
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
func merge(l1 []Operation, l2 []Operation) []Operation {
	output := append(l1, l2...)
	sort.Slice(output, func(i, j int) bool {
		return compareOperations(output[i], output[j])
	})
	return output
}

// ReceiveGossip processes incoming gossip messages from peers and updates the server's state.
func (s *Server) ReceiveGossip(request *GossipRequest, reply *GossipReply) error {

	s.PendingOperations = merge(request.Operations, s.PendingOperations)
	i := 0
	for i < len(s.PendingOperations) {
		if oneOff(s.OperationsPerformed[len(s.OperationsPerformed)-1], s.PendingOperations[i]) {
			s.OperationsPerformed = append(s.OperationsPerformed, s.PendingOperations[i])
		} else {
			break
		}
		i += 1
	}
	s.PendingOperations = s.PendingOperations[i:]

	sort.Slice(s.OperationsPerformed, func(i, j int) bool {
		return compareOperations(s.OperationsPerformed[i], s.OperationsPerformed[j])
	})

	s.Data = s.OperationsPerformed[len(s.OperationsPerformed)-1].Data
	s.VectorClock = operationsGetMaxVersionVector(s.OperationsPerformed)

	return nil
}

// sendGossip sends the server's operations to all peers to synchronize state.
func (s *Server) sendGossip() {
	for i := range s.Peers {
		req := &GossipRequest{ServerId: s.Id, Operations: s.OperationsPerformed}
		reply := &GossipReply{}
		protocol.Invoke(*s.Peers[i], "Server.RecieveGossip", &req, &reply)
	}
}
