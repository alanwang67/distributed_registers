package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

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
		fmt.Println("Usage: go run main.go [server|client] [id] [optional:interactive]")
		return
	}

	role := os.Args[1]
	id, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Invalid ID: %v\n", err)
		return
	}

	interactive := len(os.Args) > 3 && os.Args[3] == "interactive"

	// Load the configuration
	config, err := loadConfig("config.json")
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		return
	}

	// Create results directory
	resultsDir := "C:\\Users\\David\\Desktop\\CS\\distributed_registers\\abd\\results"
	if err := ensureResultsDir(resultsDir); err != nil {
		fmt.Printf("Error creating results directory: %v\n", err)
		return
	}

	switch role {
	case "server":
		startServer(id, config)
	case "client":
		startClient(id, config, resultsDir, interactive)
	default:
		fmt.Println("Invalid role. Use 'server' or 'client'.")
	}
}

func loadConfig(filePath string) (Config, error) {
	var config Config
	configData, err := os.ReadFile(filePath)
	if err != nil {
		return config, err
	}

	if err := json.Unmarshal(configData, &config); err != nil {
		return config, err
	}

	return config, nil
}

func ensureResultsDir(resultsDir string) error {
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		return os.MkdirAll(resultsDir, os.ModePerm)
	}
	return nil
}

func startServer(id int, config Config) {
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
}

func startClient(id int, config Config, resultsDir string, interactive bool) {
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

	if interactive {
		fmt.Println("Interactive mode enabled.")
		go func() {
			cli.RunInteractiveMode(config.Workload)
		}()
	} else {
		executeWorkload(cli, clientServers, config.Workload, resultsDir)
	}

	// Wait to keep the client running
	select {}
}

func executeWorkload(cli *client.Client, clientServers []client.ServerConfig, workload []client.Instruction, resultsDir string) {
	var throughputData, latencyData []CSVPoint
	start := time.Now()

	fmt.Println("Client: Starting workload execution...")
	for i, instr := range workload {
		operationStart := time.Now()

		fmt.Printf("Client: Executing operation %d: %v\n", i+1, instr)
		err := cli.PerformOperation(&clientServers[i%len(clientServers)], instr)
		if err != nil {
			fmt.Printf("Operation %d failed: %v\n", i+1, err)
			continue
		}

		// Measure latency
		latency := float64(time.Since(operationStart).Milliseconds())
		latencyData = append(latencyData, CSVPoint{
			X: float64(i + 1),
			Y: latency,
		})

		// Measure throughput
		elapsed := time.Since(start).Seconds()
		throughput := float64(i+1) / elapsed
		throughputData = append(throughputData, CSVPoint{
			X: elapsed,
			Y: throughput,
		})

		// Apply delay
		if instr.Delay > 0 {
			time.Sleep(instr.Delay)
		}

		fmt.Printf("Operation %d completed in %v\n", i+1, time.Since(operationStart))
	}

	fmt.Println("Client: Workload execution completed.")
	saveDataToCSV("Throughput", throughputData, filepath.Join(resultsDir, "throughput.csv"))
	saveDataToCSV("Latency", latencyData, filepath.Join(resultsDir, "latency.csv"))
}

// CSVPoint represents a data point for CSV output.
type CSVPoint struct {
	X float64
	Y float64
}

// saveDataToCSV writes data points to a CSV file.
func saveDataToCSV(title string, data []CSVPoint, filepath string) {
	file, err := os.Create(filepath)
	if err != nil {
		fmt.Printf("Error creating CSV file: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write([]string{"X", "Y"}); err != nil {
		fmt.Printf("Error writing headers to CSV file: %v\n", err)
		return
	}

	// Write data points
	for _, point := range data {
		if err := writer.Write([]string{
			fmt.Sprintf("%f", point.X),
			fmt.Sprintf("%f", point.Y),
		}); err != nil {
			fmt.Printf("Error writing data point to CSV file: %v\n", err)
			return
		}
	}

	fmt.Printf("Data saved to CSV file: %s\n", filepath)
}
