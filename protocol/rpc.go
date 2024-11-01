package protocol

import (
	"log"
	"net/rpc"
)

type Connection struct {
	Network string
	Address string
}

type ClientRequest struct {
	Id uint64
}

type ClientReply struct {
	ServerId  uint64
	SessionId uint64
}

type PeerRequest struct{}

type PeerReply struct{}

func Invoke(conn Connection, method string, args, reply any) error {
	c, err := rpc.Dial(conn.Network, conn.Address)
	if err != nil {
		log.Fatalf("trouble dialing %s: %s", conn.Address, err)
		return err
	}

	err = c.Call(method, args, reply)
	if err != nil {
		log.Fatalf("trouble calling %s: %s", method, err)
	}

	return nil
}
