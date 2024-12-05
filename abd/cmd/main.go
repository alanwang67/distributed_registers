package main

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/alanwang67/distributed_registers/abd/client"
	"github.com/alanwang67/distributed_registers/abd/protocol"
	"github.com/alanwang67/distributed_registers/abd/server"
	"github.com/alanwang67/distributed_registers/workload"
	"github.com/charmbracelet/log"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type Config struct {
	Servers  []ServerConfig         `json:"servers"`
	Clients  []ClientConfig         `json:"clients"`
	Workload []workload.Instruction `json:"workload"`
}

type ServerConfig struct {
	Network string `json:"network"`
	Address string `json:"address"`
}

type ClientConfig struct {
	ID      uint64 `json:"id"`
	Servers []int  `json:"servers"`
}

func main() {
	log.SetLevel(log.DebugLevel)

	// Read the config file
	configData, err := os.ReadFile("config.json")
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
		log.Fatalf("Usage: %s [client|server] [id]\nExample:\n  %s client 0\n  %s server 1", os.Args[0], os.Args[0], os.Args[0])
	}

	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		clientWorkload := config.Workload
		if clientWorkload == nil {
			log.Fatalf("No workload defined in config.json")
		}

		// Collect metrics
		metrics := runClientWithMetrics(id, servers, clientWorkload)

		// Save metrics and generate chart
		saveMetrics(metrics, "metrics.json")
		generateChart(metrics, "output_chart.png")
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

// runClientWithMetrics tracks latency and throughput during workload execution
func runClientWithMetrics(id uint64, servers []*protocol.Connection, workload []workload.Instruction) []Metric {
	c := client.New(id, servers)

	metrics := []Metric{}
	start := time.Now()

	for i, instr := range workload {
		startOp := time.Now()
		if instr.Type == "read" {
			_, err := c.ABDRead()
			if err != nil {
				log.Errorf("Client %d failed to read: %v", id, err)
			}
		} else {
			err := c.ABDWrite(instr.Value)
			if err != nil {
				log.Errorf("Client %d failed to write value %d: %v", id, instr.Value, err)
			}
		}
		duration := time.Since(startOp)

		metrics = append(metrics, Metric{
			OperationIndex: i + 1,
			OperationType:  instr.Type,
			Latency:        duration.Seconds(),
		})
	}

	log.Infof("Client %d completed workload in %v", id, time.Since(start))
	return metrics
}

// Metric represents a single performance metric
type Metric struct {
	OperationIndex int     `json:"operation_index"`
	OperationType  string  `json:"operation_type"`
	Latency        float64 `json:"latency"` // In seconds
}

// saveMetrics saves the metrics to a JSON file
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

// generateChart creates a latency chart and saves it as an image
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
