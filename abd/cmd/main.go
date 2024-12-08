package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/alanwang67/distributed_registers/abd/client"
	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
)

// Config structure for parsing the `config.json` file.
type Config struct {
	Servers []struct {
		ID      uint64 `json:"id"`
		Network string `json:"network"`
		Address string `json:"address"`
	} `json:"servers"`
	Workload []client.Instruction `json:"workload"`
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go [server|client] [id]")
		return
	}

	role := os.Args[1]
	id, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Invalid ID: %v\n", err)
		return
	}

	// Load the `config.json` file
	configData, err := os.ReadFile("config.json")
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return
	}

	// Parse the configuration
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		return
	}

	switch role {
	case "server":
		// Initialize and start a server
		if id >= len(config.Servers) {
			fmt.Printf("Invalid server ID: %d\n", id)
			return
		}

		serverConfig := config.Servers[id]
		var peers []*protocol.Connection
		for _, srv := range config.Servers {
			if srv.ID != uint64(id) {
				peers = append(peers, &protocol.Connection{
					Network: srv.Network,
					Address: srv.Address,
				})
			}
		}

		srv := server.NewServer(uint64(id), serverConfig.Address, peers)
		fmt.Printf("Starting server %d at %s\n", serverConfig.ID, serverConfig.Address)
		if err := srv.Start(); err != nil {
			fmt.Printf("Server %d failed to start: %v\n", id, err)
		}

	case "client":
		// Initialize and run the client
		clientServers := make([]client.ServerConfig, len(config.Servers))
		for i, srv := range config.Servers {
			clientServers[i] = client.ServerConfig{
				ID:      srv.ID,
				Network: srv.Network,
				Address: srv.Address,
			}
		}

		cli, err := client.NewClient(clientServers)
		if err != nil {
			fmt.Printf("Client initialization failed: %v\n", err)
			return
		}
		defer cli.CloseConnections()

		fmt.Println("Client: Interactive mode started.")
		cli.RunInteractiveMode(config.Workload)

	default:
		fmt.Println("Invalid role. Use 'server' or 'client'.")
	}
}
