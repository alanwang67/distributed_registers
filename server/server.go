package server

import (
	"sort"

	"github.com/alanwang67/distributed_registers/protocol"
)

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

func compareVersionVector(v1 []uint64, v2 []uint64) bool {
	for i := uint64(0); i < uint64(len(v1)); i++ {
		if v1[i] < v2[i] {
			return false
		}
	}
	return true
}

func DependencyCheck(serverData Server, request ClientRequest) bool {
	switch request.SessionType {
	case Causal:
		return compareVersionVector(serverData.VectorClock, request.WriteVector) && compareVersionVector(serverData.VectorClock, request.ReadVector)
	case MonotonicReads:
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	case MonotonicWrites:
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	case ReadYourWrites:
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
	case WritesFollowReads:
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
	default:
		panic("Unspecified session type")
	}
}

func getMaxVersionVector(lst [][]uint64) []uint64 {
	mx := lst[0]
	for i := 0; i < len(lst); i++ {
		for j := 0; j < len(lst[i]); j++ {
			if lst[i][j] > mx[j] {
				mx[j] = lst[i][j]
			}
		}
	}

	return mx
}

func operationsGetMaxVersionVector(lst []Operation) []uint64 {
	mx := lst[0].VersionVector
	for i := 0; i < len(lst); i++ {
		for j := 0; j < len(lst[i].VersionVector); j++ {
			if lst[i].VersionVector[j] > mx[j] {
				mx[j] = lst[i].VersionVector[j]
			}
		}
	}

	return mx
}

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
		reply.ReadVector = getMaxVersionVector(append([][]uint64{request.ReadVector}, s.VectorClock))
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

func concurrentVersionVectors(v1 []uint64, v2 []uint64) bool {
	return !compareVersionVector(v1, v2) && !compareVersionVector(v2, v1)
}

// we are applying it to o1's server
// (0 7 0) (1 0 0) (1 8 0) (2 0 0) (2 8) (1 0 1)
// (0 0 0) (1 0 0) (1 0 1)
// (0 0 0) (1 0 0)
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

func compareOperations(o1 Operation, o2 Operation) bool {
	if concurrentVersionVectors(o1.VersionVector, o2.VersionVector) {
		return o1.TieBreaker > o2.TieBreaker
	}
	return compareVersionVector(o1.VersionVector, o2.VersionVector)
}

func merge(l1 []Operation, l2 []Operation) []Operation {
	output := append(l1, l2...)
	sort.Slice(output, func(i, j int) bool {
		return compareOperations(output[i], output[j])
	})
	return output
}

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

func (s *Server) sendGossip() {
	for i := range s.Peers {
		req := &GossipRequest{ServerId: s.Id, Operations: s.OperationsPerformed}
		reply := &GossipReply{}
		protocol.Invoke(*s.Peers[i], "Server.RecieveGossip", &req, &reply)
	}
}
