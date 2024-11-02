package server

import (
	"net"
	"net/rpc"
	"time"

	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/charmbracelet/log"
)

type LamportTS struct {
	Time       uint64
	TieBreaker uint64
}

type Operation struct {
	OperationType uint64 // 0 for read, 1 for write
	TimeStamp     LamportTS
	SessionId     uint64
	Data          uint64
}

type ClientRequest struct {
	OperationType uint64 // 0 for read, 1 for write
	SessionId     uint64
	Data          uint64 // only for write operations
}

// we can add more if we need for the session comparisons
type ClientRequestReturn struct {
	OperationType uint64
	TimeStamp     LamportTS
	Data          uint64 // only for read operations
}

type Server struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	LatestSeenLamportTS LamportTS
	OperationsPerformed []Operation
	Data                uint64
}

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *Server {
	return &Server{
		Id:    id,
		Self:  self,
		Peers: peers,
	}
}

func CompareLamportTS(ts1 LamportTS, ts2 LamportTS) bool {
	if ts1.Time > ts2.Time {
		return true
	} else if ts1.Time < ts2.Time {
		return false
	} else if ts1.TieBreaker > ts2.TieBreaker { // tie breaker case
		return true
	} else if ts1.TieBreaker < ts2.TieBreaker {
		return false
	} else {
		panic("No two timestamps should be equal")
	}
}

// Whenever you use a TS within a code block you need to remember to use this value and not from server struct
// and also to assign this value to server when you return and also when you generate a new LamportTS
// you have to create a create a new server struct with this new ts
func GenerateLamportTS(serverData Server) LamportTS {
	generated_ts := LamportTS{Time: uint64(time.Now().Unix()), TieBreaker: serverData.Id}
	if CompareLamportTS(generated_ts, serverData.LatestSeenLamportTS) { // our TS is greater
		return generated_ts
	} else {
		return LamportTS{Time: serverData.LatestSeenLamportTS.Time + 1, TieBreaker: serverData.Id}
	}
}

func GetOperationsPerformed(serverData Server) []Operation {
	return serverData.OperationsPerformed
}

func ProcessClientRequest(serverData Server, request ClientRequest) (Server, ClientRequestReturn) {
	if request.OperationType == 0 { // reads
		ts := GenerateLamportTS(serverData)

		return Server{Id: serverData.Id, Self: serverData.Self, Peers: serverData.Peers, LatestSeenLamportTS: ts,
			OperationsPerformed: serverData.OperationsPerformed, Data: request.Data}, ClientRequestReturn{OperationType: 0, TimeStamp: ts, Data: serverData.Data}
	} else { //writes
		ts := GenerateLamportTS(serverData)

		operations := append(serverData.OperationsPerformed, Operation{OperationType: 1, TimeStamp: ts, SessionId: request.SessionId, Data: request.Data})
		return Server{Id: serverData.Id, Self: serverData.Self, Peers: serverData.Peers, LatestSeenLamportTS: ts,
			OperationsPerformed: operations, Data: request.Data}, ClientRequestReturn{OperationType: 1, TimeStamp: ts}
	}
}

func (s *Server) Start() error {
	log.Debugf("starting server %d", s.Id)

	l, err := net.Listen(s.Self.Network, s.Self.Address)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Debugf("server %d listening on %s", s.Id, s.Self.Address)

	rpc.Register(s)

	for {
		rpc.Accept(l)
		// some other stuff goes here...

	}
}
