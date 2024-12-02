package protocol

import (
	"context"
	"net"
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
		// log.Fatalf("trouble dialing %s: %s", conn.Address, err)
		return err
	}

	err = c.Call(method, args, reply)
	if err != nil {
		// log.Fatalf("trouble calling %s: %s", method, err)
	}

	return nil
}

func DialContext(ctx context.Context, network, address string) (*rpc.Client, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}
