package main

import (
	"embed"
	"encoding/json"
	"os"
	"strconv"

	"github.com/alanwang67/distributed_registers/abd/client"
	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"github.com/alanwang67/distributed_registers/abd/workload"
	"github.com/charmbracelet/log"
)

//go:embed config.json
var f embed.FS

type Config struct {
	Servers []ServerConfig `json:"servers"`
	Clients []ClientConfig `json:"clients"`
}

type ServerConfig struct {
	Network string `json:"network"`
	Address string `json:"address"`
}

type ClientConfig struct {
	ClientId uint64                 `json:"client_id"`
	Workload []workload.Instruction `json:"workload"`
}

func main() {
	log.SetLevel(log.DebugLevel)

	configData, err := f.ReadFile("config.json")
	if err != nil {
		log.Fatalf("can't read config.json: %s", err)
	}

	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("can't unmarshal JSON: %s", err)
	}

	servers := make([]*protocol.Connection, len(config.Servers))
	for i, s := range config.Servers {
		servers[i] = &protocol.Connection{
			Network: s.Network,
			Address: s.Address,
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
		var clientWorkload []workload.Instruction
		// Find the workload for this client
		for _, clientConfig := range config.Clients {
			if clientConfig.ClientId == id {
				clientWorkload = clientConfig.Workload
				break
			}
		}
		if clientWorkload == nil {
			log.Fatalf("No workload found for client %d", id)
		}
		err := client.New(id, servers).Start(clientWorkload)
		if err != nil {
			log.Fatalf("Client %d encountered an error: %v", id, err)
		}
	case "server":
		if id >= uint64(len(servers)) {
			log.Fatalf("Invalid server id %d", id)
		}
		err := server.New(id, servers[id], servers).Start()
		if err != nil {
			log.Fatalf("Server %d encountered an error: %v", id, err)
		}
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
