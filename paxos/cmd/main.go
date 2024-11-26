package main

import (
	"embed"
	"encoding/json"
	"os"
	"strconv"

	"github.com/alanwang67/distributed_registers/paxos/client"
	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/server"
	"github.com/charmbracelet/log"
)

//go:embed config.json
var f embed.FS

func main() {
	log.SetLevel(log.DebugLevel)

	config, err := f.ReadFile("config.json")
	if err != nil {
		log.Fatalf("can't read config.json: %s", err)
	}

	var data map[string]interface{}
	err = json.Unmarshal(config, &data)
	if err != nil {
		log.Fatalf("can't unmarshal JSON: %s", err)
	}

	servers := make([]*protocol.Connection, len(data["servers"].([]interface{})))
	for i, s := range data["servers"].([]interface{}) {
		conn, ok := s.(map[string]interface{})
		if !ok {
			log.Fatalf("invalid server data at index %d", i)
		}

		network, _ := conn["network"].(string)
		address, _ := conn["address"].(string)

		servers[i] = &protocol.Connection{
			Network: network,
			Address: address,
		}
	}

	if len(os.Args) < 3 {
		log.Fatalf("usage: %s [client|server] [id]", os.Args[0])
	}

	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		client.New(id, servers).Start() // TODO: make the servers available to client match the config
	case "server":
		server.New(id, servers[id], servers).Start()
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
