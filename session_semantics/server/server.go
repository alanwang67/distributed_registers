package server

import (
	"log"
	"reflect"
	"sort"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/vectorclock"
)

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	s := &Server{
		Id:                  id,
		Self:                self,
		Peers:               peers,
		VectorClock:         make([]uint64, len(peers)),
		OperationsPerformed: []Operation{},
		MyOperations:        []Operation{},
		PendingOperations:   []Operation{},
		Data:                0,
	}
	go s.sendGossip()
	log.Printf("[INFO] Server %d initialized", id)
	return s
}

func (s *Server) ProcessClientRequest(request *ClientRequest, reply *ClientReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[INFO] Server %d received client request: %+v", s.Id, request)

	if !DependencyCheck(s.VectorClock, *request) {
		log.Printf("[WARN] Server %d dependency check failed for client request", s.Id)
		reply.Succeeded = false
		return nil
	}

	if request.OperationType == Read {
		log.Printf("[INFO] Server %d processing read request", s.Id)
		reply.Succeeded = true
		reply.OperationType = Read
		reply.Data = s.Data
		reply.ReadVector = vectorclock.GetMaxVersionVector(append([][]uint64{request.ReadVector}, append([]uint64(nil), s.VectorClock...)))
		reply.WriteVector = request.WriteVector
	} else {
		log.Printf("[INFO] Server %d processing write request with value %d", s.Id, request.Data)
		s.VectorClock[s.Id]++
		op := Operation{
			OperationType: Write,
			VersionVector: append([]uint64(nil), s.VectorClock...),
			TieBreaker:    s.Id,
			Data:          request.Data,
		}
		s.OperationsPerformed = append(s.OperationsPerformed, op)
		s.MyOperations = append(s.MyOperations, op)
		s.Data = request.Data
		reply.Succeeded = true
		reply.OperationType = Write
		reply.Data = request.Data
		reply.ReadVector = request.ReadVector
		reply.WriteVector = append([]uint64(nil), s.VectorClock...)
	}

	log.Printf("[INFO] Server %d updated vector clock: %+v", s.Id, s.VectorClock)
	return nil
}

func DependencyCheck(vectorClock []uint64, request ClientRequest) bool {
	switch request.SessionType {
	case Causal:
		return vectorclock.CompareVersionVector(vectorClock, request.WriteVector) &&
			vectorclock.CompareVersionVector(vectorClock, request.ReadVector)
	case MonotonicReads:
		return vectorclock.CompareVersionVector(vectorClock, request.ReadVector)
	case MonotonicWrites:
		return vectorclock.CompareVersionVector(vectorClock, request.WriteVector)
	case ReadYourWrites:
		return vectorclock.CompareVersionVector(vectorClock, request.WriteVector)
	case WritesFollowReads:
		return vectorclock.CompareVersionVector(vectorClock, request.ReadVector)
	default:
		log.Printf("[ERROR] Unspecified session type in dependency check")
		return false
	}
}

func oneOffVersionVector(serverId uint64, v1, v2 []uint64) bool {
	for i := range v1 {
		if i == int(serverId) {
			continue
		}
		if v1[i] > v2[i] {
			return false
		}
	}
	return true
}

func compareOperations(o1, o2 Operation) bool {
	if vectorclock.ConcurrentVersionVectors(o1.VersionVector, o2.VersionVector) {
		return o1.TieBreaker > o2.TieBreaker
	}
	return vectorclock.CompareVersionVector(o1.VersionVector, o2.VersionVector)
}

func removeDuplicateOperationsAndSort(ops []Operation) []Operation {
	sort.Slice(ops, func(i, j int) bool {
		return compareOperations(ops[j], ops[i])
	})
	uniqueOps := []Operation{}
	for i, op := range ops {
		if i == 0 || !reflect.DeepEqual(op, ops[i-1]) {
			uniqueOps = append(uniqueOps, op)
		}
	}
	return uniqueOps
}

func (s *Server) ReceiveGossip(request *GossipRequest, reply *GossipReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[INFO] Server %d received gossip from Server %d", s.Id, request.ServerId)

	s.PendingOperations = mergePendingOperations(request.Operations, s.PendingOperations)
	log.Printf("[INFO] Server %d merged pending operations, total pending: %d", s.Id, len(s.PendingOperations))

	latestVersionVector := vectorclock.GetMaxVersionVector([][]uint64{s.VectorClock})

	i := 0
	for i < len(s.PendingOperations) {
		op := s.PendingOperations[i]
		if vectorclock.CompareVersionVector(latestVersionVector, op.VersionVector) ||
			oneOffVersionVector(s.Id, latestVersionVector, op.VersionVector) {
			log.Printf("[INFO] Server %d applying operation: %+v", s.Id, op)
			s.OperationsPerformed = append(s.OperationsPerformed, op)
			latestVersionVector = vectorclock.GetMaxVersionVector([][]uint64{latestVersionVector, op.VersionVector})
			i++
		} else {
			log.Printf("[WARN] Server %d cannot yet apply operation: %+v", s.Id, op)
			break
		}
	}

	s.PendingOperations = s.PendingOperations[i:]
	log.Printf("[INFO] Server %d updated vector clock: %+v", s.Id, s.VectorClock)
	return nil
}

func (s *Server) sendGossip() {
	for {
		time.Sleep(50 * time.Millisecond)
		if len(s.MyOperations) == 0 {
			continue
		}
		log.Printf("[INFO] Server %d sending gossip", s.Id)
		for _, peer := range s.Peers {
			req := &GossipRequest{ServerId: s.Id, Operations: s.MyOperations}
			reply := &GossipReply{}
			protocol.Invoke(*peer, "Server.ReceiveGossip", req, reply)
		}
		s.MyOperations = []Operation{}
	}
}

func (s *Server) PrintOperations(request *ClientRequest, reply *ClientReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[INFO] Server %d operations performed: %+v", s.Id, s.OperationsPerformed)
	return nil
}

func mergePendingOperations(l1, l2 []Operation) []Operation {
	output := append(l1, l2...)
	return removeDuplicateOperationsAndSort(output)
}
