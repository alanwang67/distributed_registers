package server

import (
	"log"
	"math/rand"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
)

func (s *Server) HandleClientRequest(req *protocol.ClientRequest, reply *protocol.ClientReply) error {
	log.Printf("[DEBUG] server %d handling client request %d", s.Id, req.Id)

	*reply = protocol.ClientReply{
		ServerId:  s.Id,
		SessionId: uint64(rand.Uint32())<<32 + uint64(rand.Uint32()),
	}
	log.Printf("[DEBUG] server %d replied to client %d with session %d", s.Id, req.Id, reply.SessionId)

	return nil
}
