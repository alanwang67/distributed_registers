package main

import (
	"os"
	"strconv"

	"github.com/alanwang67/distributed_registers/client"
	"github.com/alanwang67/distributed_registers/protocol"
	"github.com/alanwang67/distributed_registers/server"
	"github.com/charmbracelet/log"
)

func main() {
	log.SetLevel(log.DebugLevel)

	conn := &protocol.Connection{
		Network: "tcp",
		Address: "localhost:1234",
	}

	switch os.Args[1] {
	case "client":
		cid, err := strconv.ParseUint(os.Args[2], 10, 64)
		if err != nil {
			log.Fatalf("trouble converting %s to int: %s", os.Args[2], err)
		}

		client.New(cid, []*protocol.Connection{conn}).Start()
	case "server":
		server.New(1, conn, nil).Start()
	}
}
