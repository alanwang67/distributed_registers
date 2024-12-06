package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alanwang67/distributed_registers/session_semantics/client"
	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
	"github.com/charmbracelet/log"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// Metric represents a single performance metric
type Metric struct {
	OperationIndex int     `json:"operation_index"`
	OperationType  string  `json:"operation_type"`
	Latency        float64 `json:"latency"` // In seconds
}

// Config structure for loading config.json
type Config struct {
	Servers  []serverConfig   `json:"servers"`
	Clients  []clientConfig   `json:"clients"`
	Workload []WorkloadConfig `json:"workloads"`
}

// serverConfig contains details about each server
type serverConfig struct {
	Id      uint64 `json:"id"`
	Network string `json:"network"`
	Address string `json:"address"`
}

// clientConfig contains client-server mapping
type clientConfig struct {
	Id      uint64   `json:"id"`
	Servers []uint64 `json:"servers"`
}

// WorkloadConfig defines the structure for workload operations
type WorkloadConfig struct {
	Type  string `json:"Type"`
	Value uint64 `json:"Value"`
	Delay int    `json:"Delay"`
}

func main() {
	// Set log level to debug
	log.SetLevel(log.DebugLevel)

	// Log command-line arguments
	log.Infof("Arguments: %v", os.Args)

	// Check for correct number of arguments
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s [client|server] [id]", os.Args[0])
	}

	// Get the current working directory
	exeDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error getting current directory: %v", err)
	}
	log.Debugf("Current directory: %s", exeDir)

	// Construct the path to config.json in the same directory as main.go
	configFile := filepath.Join(exeDir, "config.json")

	// Read the config file
	configData, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Can't read config.json: %s", err)
	}

	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("Can't unmarshal JSON: %s", err)
	}

	// Initialize servers from the config
	servers := make([]*protocol.Connection, len(config.Servers))
	for i, s := range config.Servers {
		servers[i] = &protocol.Connection{
			Network: s.Network,
			Address: s.Address,
		}
	}

	// Parse the ID from command-line arguments
	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("Can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		// Run the client and collect metrics
		metrics := runClientWithMetrics(id, servers, config.Workload)

		// Save metrics and generate chart
		saveMetrics(metrics, "metrics.json")
		generateChart(metrics, "output_chart.png")

	case "server":
		if id >= uint64(len(servers)) {
			log.Fatalf("Invalid server id %d", id)
		}
		// Start the server for the given id
		err := server.New(id, servers[id], servers).Start()
		if err != nil {
			log.Fatalf("Server %d encountered an error: %v", id, err)
		}

	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}

// runClientWithMetrics executes the workload and tracks performance metrics
func runClientWithMetrics(id uint64, servers []*protocol.Connection, workload []WorkloadConfig) []Metric {
	c := client.New(id, servers)

	metrics := []Metric{}
	for i, op := range workload {
		startOp := time.Now()

		switch op.Type {
		case "read":
			resp := c.ReadFromServer(server.Causal)
			log.Infof("Client %d performed read operation: Response = %v", id, resp)
		case "write":
			resp := c.WriteToServer(op.Value, server.Causal)
			log.Infof("Client %d performed write operation with value %d: Response = %v", id, op.Value, resp)
		default:
			log.Warnf("Client %d encountered unknown operation type: %s", id, op.Type)
			continue
		}

		duration := time.Since(startOp)

		metrics = append(metrics, Metric{
			OperationIndex: i + 1,
			OperationType:  op.Type,
			Latency:        duration.Seconds(),
		})

		if op.Delay > 0 {
			time.Sleep(time.Duration(op.Delay) * time.Millisecond)
		}
	}

	log.Infof("Client %d completed workload", id)
	return metrics
}

// saveMetrics writes the collected metrics to a JSON file
func saveMetrics(metrics []Metric, filename string) {
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		log.Fatalf("Failed to serialize metrics: %v", err)
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Fatalf("Failed to write metrics to file: %v", err)
	}
	log.Infof("Metrics saved to %s", filename)
}

// generateChart creates a chart of operation latencies and saves it to a file
func generateChart(metrics []Metric, filename string) {
	points := make(plotter.XYs, len(metrics))
	for i, metric := range metrics {
		points[i].X = float64(metric.OperationIndex)
		points[i].Y = metric.Latency
	}

	p := plot.New()
	p.Title.Text = "Operation Latency"
	p.X.Label.Text = "Operation Index"
	p.Y.Label.Text = "Latency (s)"

	line, err := plotter.NewLine(points)
	if err != nil {
		log.Fatalf("Failed to create plot line: %v", err)
	}
	p.Add(line)

	if err := p.Save(8*vg.Inch, 4*vg.Inch, filename); err != nil {
		log.Fatalf("Failed to save chart: %v", err)
	}
	log.Infof("Chart saved to %s", filename)
}
