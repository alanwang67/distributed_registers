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

func GetOperationsPerformed(serverData Server) []Operation {
	return serverData.OperationsPerformed
}

func DependencyCheck(serverData Server, request ClientRequest) bool {
	if request.SessionType == 0 {
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
	} else if request.SessionType == 1 {
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	} else if request.SessionType == 2 {
		return compareVersionVector(serverData.VectorClock, request.ReadVector)
	} else if request.SessionType == 3 {
		return compareVersionVector(serverData.VectorClock, request.WriteVector)
	}
}

func getMaxVersionVector(log []Operation) []uint64 {
	mx := log[0].VersionVector
	for i := 0; i < len(log); i++ {
		if compareVersionVector(log[i].VersionVector, mx) {
			mx = log[i].VersionVector
		}
	}
	return mx
}

func ProcessClientRequest(serverData Server, request ClientRequest) (Server, ClientReply) {
	if !(DependencyCheck(serverData, request)) {
		return serverData, ClientReply{Succeeded: false}
	}

	if request.OperationType == 0 { // reads
		lastElem := len(serverData.OperationsPerformed) - 1
		ReadVector := serverData.OperationsPerformed[lastElem].VersionVector
		if compareVersionVector(request.ReadVector, serverData.OperationsPerformed[lastElem].VersionVector) {
			ReadVector = request.ReadVector
		}

		return Server{Id: serverData.Id, Self: serverData.Self, Peers: serverData.Peers, VectorClock: serverData.VectorClock,
				OperationsPerformed: serverData.OperationsPerformed, Data: serverData.Data}, ClientReply{Succeeded: true, OperationType: 0, Data: serverData.Data,
				ReadVector: ReadVector, WriteVector: request.WriteVector}
	} else { //writes
		vs := generateVersionVector(serverData)

		operations := append(serverData.OperationsPerformed, Operation{OperationType: 1, VersionVector: vs, Data: request.Data})

		return Server{Id: serverData.Id, Self: serverData.Self, Peers: serverData.Peers, VectorClock: vs,
				OperationsPerformed: operations, Data: request.Data}, ClientReply{Succeeded: true, OperationType: 1, Data: request.Data,
				ReadVector: request.ReadVector, WriteVector: vs}
	}
}

func RecieveGossip(serverData Server, gossipData ServerGossipRequest) Server {
	i := 0
	intermediateList := serverData.OperationsPerformed

	for j := 0; i < len(gossipData.Operations); i++ {
		if compareVersionVector(gossipData.Operations[j].VersionVector, intermediateList[i].VersionVector) {
			intermediateList = append(intermediateList[:i+1], intermediateList[i:]...)
			intermediateList[i] = gossipData.Operations[j]
		}
	}

	data := intermediateList[len(intermediateList)-1].Data

	return Server{Id: serverData.Id, Self: serverData.Self, Peers: serverData.Peers, VectorClock: getMaxVersionVector(serverData.OperationsPerformed),
		OperationsPerformed: intermediateList, Data: data}
}
