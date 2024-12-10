package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/alanwang67/distributed_registers/abd/client"
	"github.com/alanwang67/distributed_registers/abd/server"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// Config structure for parsing the `config.json` file.
type Config struct {
	Servers []struct {
		ID      int    `json:"id"`
		Network string `json:"network"`
		Address string `json:"address"`
	} `json:"servers"`
	Workload []struct {
		Type  string `json:"type"`
		Value *int   `json:"value"` // Use pointer to allow nil values for reads
		Delay int    `json:"delay"`
	} `json:"workload"`
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go [server|client] [id]")
		os.Exit(1)
	}

	role := os.Args[1]
	id, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid ID: %v\n", err)
	}

	// Load and parse the `config.json` file
	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Error reading config file: %v\n", err)
	}

	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Error parsing config file: %v\n", err)
	}

	switch role {
	case "server":
		runServer(id, config)
	case "client":
		runClient(id, config)
	default:
		log.Fatalf("Invalid role. Use 'server' or 'client'.\n")
	}
}

func runServer(id int, config Config) {
	// Validate server ID
	if id < 0 || id >= len(config.Servers) {
		log.Fatalf("Invalid server ID: %d\n", id)
	}

	serverConfig := config.Servers[id]

	// Collect peer servers
	var peers []*server.ServerConfig
	for _, srv := range config.Servers {
		if srv.ID != id {
			peers = append(peers, &server.ServerConfig{
				ID:      srv.ID,
				Network: srv.Network,
				Address: srv.Address,
			})
		}
	}

	// Initialize and start the server
	srv := server.NewServer(id, serverConfig.Address, peers)
	log.Printf("[Server %d] Starting at %s with peers: %v", id, serverConfig.Address, peers)
	if err := srv.Start(); err != nil {
		log.Fatalf("[Server %d] Failed to start: %v\n", id, err)
	}
}

func runClient(id int, config Config) {
	// Validate client ID
	if id < 0 {
		log.Fatalf("Invalid client ID: %d\n", id)
	}

	// Rotate servers so that each client starts with a designated server
	clientServers := make([]map[string]interface{}, len(config.Servers))
	for i, srv := range config.Servers {
		clientServers[i] = map[string]interface{}{
			"id":      srv.ID,
			"network": srv.Network,
			"address": srv.Address,
		}
	}

	// Initialize the client
	cli := &client.Client{
		ID:      id,
		Servers: clientServers,
	}

	// Initialize metrics tracking
	var latencies []float64
	var timestamps []float64
	startTime := time.Now()

	// Execute the workload
	log.Printf("[Client %d] Starting workload execution.", id)
	for _, task := range config.Workload {
		operationStart := time.Now()
		switch task.Type {
		case "read":
			log.Printf("[Client %d] Executing read operation.", id)
			cli.Read()
		case "write":
			if task.Value == nil {
				log.Printf("[Client %d] Write task missing value, skipping.", id)
				continue
			}
			log.Printf("[Client %d] Executing write operation with value=%d.", id, *task.Value)
			cli.Write(*task.Value)
		default:
			log.Printf("[Client %d] Unknown task type: %s", id, task.Type)
		}

		// Record latency
		operationDuration := time.Since(operationStart).Seconds()
		latencies = append(latencies, operationDuration)

		// Record timestamp relative to the start of the workload
		timestamps = append(timestamps, time.Since(startTime).Seconds())

		// Apply delay if specified
		if task.Delay > 0 {
			log.Printf("[Client %d] Applying delay: %dms.", id, task.Delay)
			time.Sleep(time.Duration(task.Delay) * time.Millisecond)
		}
	}
	log.Printf("[Client %d] Workload execution completed.", id)

	// Generate charts
	generateLatencyChart(timestamps, latencies)
	generateThroughputChart(timestamps)
}

func generateLatencyChart(timestamps, latencies []float64) {
	points := make(plotter.XYs, len(timestamps))
	for i := range timestamps {
		points[i].X = timestamps[i]
		points[i].Y = latencies[i]
	}

	p := plot.New()
	p.Title.Text = "Latency Chart"
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Latency (s)"

	line, err := plotter.NewLine(points)
	if err != nil {
		log.Fatalf("Error creating line for latency chart: %v", err)
	}
	p.Add(line)

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "latency_chart.png"); err != nil {
		log.Fatalf("Error saving latency chart: %v", err)
	}
	log.Println("Latency chart saved to latency_chart.png")
}

func generateThroughputChart(timestamps []float64) {
	points := make(plotter.XYs, len(timestamps))
	for i := range timestamps {
		points[i].X = timestamps[i]
		points[i].Y = float64(i+1) / timestamps[i] // Throughput = operations / time
	}

	p := plot.New()
	p.Title.Text = "Throughput Chart"
	p.X.Label.Text = "Time (s)"
	p.Y.Label.Text = "Throughput (ops/s)"

	line, err := plotter.NewLine(points)
	if err != nil {
		log.Fatalf("Error creating line for throughput chart: %v", err)
	}
	p.Add(line)

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "throughput_chart.png"); err != nil {
		log.Fatalf("Error saving throughput chart: %v", err)
	}
	log.Println("Throughput chart saved to throughput_chart.png")
}
