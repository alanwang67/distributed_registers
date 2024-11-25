package server

import (
	"math/rand/v2"

	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/charmbracelet/log"
)

func (s *Server) HandleClientRequest(req *protocol.ClientRequest, reply *protocol.ClientReply) error {
	log.Debugf("server %d handling client request %d", s.Id, req.Id)

	*reply = protocol.ClientReply{
		ServerId:  s.Id,
		SessionId: uint64(rand.Uint32())<<32 + uint64(rand.Uint32()),
	}
	log.Debugf("server %d replied to client %d with session %d", s.Id, req.Id, reply.SessionId)

	return nil
}
