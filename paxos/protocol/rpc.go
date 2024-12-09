package protocol

import "net/rpc"

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

// Invoke performs an RPC call to the given method on the specified connection.
func Invoke(conn Connection, method string, args, reply any) error {
	c, err := rpc.Dial(conn.Network, conn.Address)
	if err != nil {
		return err
	}

	err = c.Call(method, args, reply)
	if err != nil {
		return err
	}

	return nil
}
