package main
import (
	"log"
	"fmt"
	"net/rpc"
)


type Args struct {
	A, B int
}

func main() {
	client, err := rpc.DialHTTP("tcp", ":1235")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call
	args := Args{7,8}
	var reply int64
	err = client.Call("Server.LamportTimestamp", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d*%d=%d", reply)
}
