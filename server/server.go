package server

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type Server struct {
	id int
	serverPorts
	lamportClock int64
	relevantWrites
	relevantReads
	data int
}

type Args struct {
	A, B int
}

// have to have a function that keeps lamportClock
func (s *Server) LamportTimeStamp(args *Args, reply *int64) error {
	*reply = s.lamportClock
	return nil
}

func (s *Server) server() {
	rpc.Register(s)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":1235")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)

}

func (s *Server) generateTimeStamp() {
}

func Make(serverNumber int) *Server {
	s := Server{id: serverNumber, lamportClock: (time.Now().Unix() * 10) + int64(serverNumber)}
	s.server()
	return &s
}
