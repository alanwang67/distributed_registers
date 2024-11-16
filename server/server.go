package server

import (
	"github.com/alanwang67/distributed_registers/protocol"
)

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	return &Server{
		Id:          id,
		Self:        self,
		Peers:       peers,
		VectorClock: make([]uint64, len(peers)),
	}
}

func generateVersionVector(serverData Server) []uint64 {
	serverData.VectorClock[serverData.Id] += 1
	return serverData.VectorClock
}

// this means v1 is greater than v2
func compareVersionVector(v1 []uint64, v2 []uint64) bool {
	for i := uint64(0); i < uint64(len(v1)); i++ {
		if v1[i] < v2[i] {
			return false
		}
	}
	return true
}

func sameOperation(v1 []uint64, v2 []uint64) bool {
	for i := uint64(0); i < uint64(len(v1)); i++ {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}

func GetOperationsPerformed(serverData Server) []Operation {
	return serverData.OperationsPerformed
}

func DependencyCheck(serverData Server, request ClientRequest) bool {
	switch request.SessionType {
	case MonotonicReads:
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	case MonotonicWrites:
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	case ReadYourWrites:
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
	case WritesFollowReads:
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
		// case 4 undefined
		// return compareVersionVector(serverData.VectorClock, request.WriteVector) && compareVersionVector(serverData.VectorClock, request.ReadVector)
	default:
		panic("Unspecified session type")
	}
}

func getMaxVersionVector(log []Operation) []uint64 {
	mx := log[0].VersionVector
	for i := 0; i < len(log); i++ {
		for j := 0; j < len(log[i].VersionVector); j++ {
			if log[i].VersionVector[j] > mx[j] {
				mx[j] = log[i].VersionVector[j]
			}
		}
	}
	return mx
}

func ProcessClientRequest(serverData Server, request ClientRequest) (Server, ClientReply) {
	if !(DependencyCheck(serverData, request)) {
		return serverData, ClientReply{Succeeded: false}
	}

	if request.OperationType == Read {
		lastElem := len(serverData.OperationsPerformed) - 1
		ReadVector := serverData.OperationsPerformed[lastElem].VersionVector
		if compareVersionVector(
			request.ReadVector,
			serverData.OperationsPerformed[lastElem].VersionVector,
		) {
			ReadVector = request.ReadVector
		}

		return Server{
				Id:                  serverData.Id,
				Self:                serverData.Self,
				Peers:               serverData.Peers,
				VectorClock:         serverData.VectorClock,
				OperationsPerformed: serverData.OperationsPerformed,
				Data:                serverData.Data,
			},
			ClientReply{
				Succeeded:     true,
				OperationType: Read,
				Data:          serverData.Data,
				ReadVector:    ReadVector,
				WriteVector:   request.WriteVector,
			}
	} else { // writes
		vs := generateVersionVector(serverData)

		operations := append(
			serverData.OperationsPerformed,
			Operation{
				OperationType: Write,
				VersionVector: vs,
				Data:          request.Data,
			},
		)

		return Server{
				Id:                  serverData.Id,
				Self:                serverData.Self,
				Peers:               serverData.Peers,
				VectorClock:         vs,
				OperationsPerformed: operations,
				Data:                request.Data,
			},
			ClientReply{
				Succeeded:     true,
				OperationType: Write,
				Data:          request.Data,
				ReadVector:    request.ReadVector,
				WriteVector:   vs,
			}
	}
}

// there might be an issue with this
func ReceiveGossip(serverData Server, gossipData ServerGossipRequest) Server {
	intermediateList := serverData.OperationsPerformed
	j := 0

	for i := 0; i < len(intermediateList); i++ {
		for sameOperation(gossipData.Operations[j].VersionVector, intermediateList[i].VersionVector) {
			j += 1
		}
		if compareVersionVector(gossipData.Operations[j].VersionVector, intermediateList[i].VersionVector) {
			intermediateList = append(intermediateList[:i+1], intermediateList[i:]...)
			intermediateList[i] = gossipData.Operations[j]
			j += 1
		}
	}

	intermediateList = append(intermediateList, gossipData.Operations[j:]...)

	data := intermediateList[len(intermediateList)-1].Data

	return Server{
		Id:                  serverData.Id,
		Self:                serverData.Self,
		Peers:               serverData.Peers,
		VectorClock:         getMaxVersionVector(intermediateList),
		OperationsPerformed: intermediateList,
		Data:                data,
	}
}

func (s *Server) handleNetworkCalls(req *ClientRequest, reply *ClientReply) {

}
