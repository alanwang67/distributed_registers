package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alanwang67/distributed_registers/abd/client"
	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
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
	var throughputData, latencyData plotter.XYs
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
		latencyData = append(latencyData, plotter.XY{
			X: float64(i + 1),
			Y: latency,
		})

		// Measure throughput
		elapsed := time.Since(start).Seconds()
		throughput := float64(i+1) / elapsed
		throughputData = append(throughputData, plotter.XY{
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
	generateChart("Throughput", "Time (s)", "Throughput (operations/s)", throughputData, filepath.Join(resultsDir, "throughput.png"))
	generateChart("Latency", "Operation", "Latency (ms)", latencyData, filepath.Join(resultsDir, "latency.png"))
}

// generateChart creates and saves a PNG chart.
func generateChart(title, xLabel, yLabel string, data plotter.XYs, filepath string) {
	p := plot.New()
	p.Title.Text = title
	p.X.Label.Text = xLabel
	p.Y.Label.Text = yLabel

	line, err := plotter.NewLine(data)
	if err != nil {
		fmt.Printf("Error creating line plot: %v\n", err)
		return
	}
	p.Add(line)

	if err := p.Save(10*vg.Inch, 4*vg.Inch, filepath); err != nil {
		fmt.Printf("Error saving plot: %v\n", err)
	}
}
