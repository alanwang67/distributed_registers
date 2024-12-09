package main

import (
	"embed"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/alanwang67/distributed_registers/paxos/client"
	"github.com/alanwang67/distributed_registers/paxos/protocol"
	"github.com/alanwang67/distributed_registers/paxos/sequencer"
	"github.com/alanwang67/distributed_registers/paxos/server"
)

//go:embed config.json
var f embed.FS

func main() {
	config, err := f.ReadFile("config.json")
	if err != nil {
		log.Fatalf("[ERROR] can't read config.json: %s", err)
	}

	var data map[string]interface{}
	err = json.Unmarshal(config, &data)
	if err != nil {
		log.Fatalf("[ERROR] can't unmarshal JSON: %s", err)
	}

	serversData, ok := data["servers"].([]interface{})
	if !ok {
		log.Fatalf("[ERROR] 'servers' key not found or invalid in config.")
	}
	servers := make([]*protocol.Connection, len(serversData))
	for i, s := range serversData {
		conn, ok := s.(map[string]interface{})
		if !ok {
			log.Fatalf("[ERROR] invalid server data at index %d", i)
		}

		network, _ := conn["network"].(string)
		address, _ := conn["address"].(string)

		servers[i] = &protocol.Connection{
			Network: network,
			Address: address,
		}
	}

	sequencersData, ok := data["sequencer"].([]interface{})
	if !ok {
		log.Fatalf("[ERROR] 'sequencer' key not found or invalid in config.")
	}
	sequencers := make([]*protocol.Connection, len(sequencersData))
	for i, s := range sequencersData {
		conn, ok := s.(map[string]interface{})
		if !ok {
			log.Fatalf("[ERROR] invalid sequencer data at index %d", i)
		}

		network, _ := conn["network"].(string)
		address, _ := conn["address"].(string)

		sequencers[i] = &protocol.Connection{
			Network: network,
			Address: address,
		}
	}

	if len(os.Args) < 3 {
		log.Fatalf("[ERROR] usage: %s [client|server|sequencer] [id]", os.Args[0])
	}

	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("[ERROR] can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		log.Printf("[INFO] Starting client %d", id)
		err := client.New(id, servers, sequencers).Start()
		if err != nil {
			log.Printf("[ERROR] Client %d failed: %v", id, err)
		}
	case "server":
		if int(id) >= len(servers) {
			log.Fatalf("[ERROR] Invalid server ID: %d", id)
		}
		log.Printf("[INFO] Starting server %d at %s", id, servers[id].Address)
		err := server.New(id, servers[id], servers).Start()
		if err != nil {
			log.Printf("[ERROR] Server %d failed: %v", id, err)
		}
	case "sequencer":
		if int(id) >= len(sequencers) {
			log.Fatalf("[ERROR] Invalid sequencer ID: %d", id)
		}
		log.Printf("[INFO] Starting sequencer %d at %s", id, sequencers[id].Address)
		err := sequencer.New(sequencers[id]).Start()
		if err != nil {
			log.Printf("[ERROR] Sequencer %d failed: %v", id, err)
		}
	default:
		log.Fatalf("[ERROR] unknown command: %s", os.Args[1])
	}
}
