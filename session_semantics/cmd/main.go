package main

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"

	"github.com/alanwang67/distributed_registers/session_semantics/client"
	"github.com/alanwang67/distributed_registers/session_semantics/protocol"
	"github.com/alanwang67/distributed_registers/session_semantics/server"
)

// Metric represents a single performance metric
type Metric struct {
	OperationIndex int     `json:"operation_index"`
	OperationType  string  `json:"operation_type"`
	Latency        float64 `json:"latency"`   // In seconds
	Timestamp      float64 `json:"timestamp"` // Time since start in seconds
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
	if len(os.Args) < 3 {
		log.Fatalf("[ERROR] Usage: %s [client|server] [id]", os.Args[0])
	}

	exeDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("[ERROR] Error getting current directory: %v", err)
	}

	configFile := filepath.Join(exeDir, "config.json")
	configData, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("[ERROR] Can't read config.json: %s", err)
	}

	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("[ERROR] Can't unmarshal JSON: %s", err)
	}

	servers := make([]*protocol.Connection, len(config.Servers))
	for i, s := range config.Servers {
		servers[i] = &protocol.Connection{
			Network: s.Network,
			Address: s.Address,
		}
	}

	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("[ERROR] Can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		metrics := runClientWithMetrics(id, servers, config.Workload)
		saveMetrics(metrics, "metrics.json")
		saveMetricsToCSV(metrics, "latency.csv", "throughput.csv")
		plotMetrics(metrics, "latency_plot.png", "throughput_plot.png")

	case "server":
		if id >= uint64(len(servers)) {
			log.Fatalf("[ERROR] Invalid server id %d", id)
		}
		log.Printf("[INFO] Starting server %d at %s", id, servers[id].Address)
		err := server.New(id, servers[id], servers).Start()
		if err != nil {
			log.Fatalf("[ERROR] Server %d encountered an error: %v", id, err)
		}

	default:
		log.Fatalf("[ERROR] Unknown command: %s", os.Args[1])
	}
}

func runClientWithMetrics(id uint64, servers []*protocol.Connection, workload []WorkloadConfig) []Metric {
	c := client.New(id, servers)

	startTime := time.Now()
	metrics := []Metric{}

	for i, op := range workload {
		startOp := time.Now()

		switch op.Type {
		case "read":
			resp := c.ReadFromServer(server.WritesFollowReads)
			log.Printf("[INFO] Client %d performed read operation: Response = %v", id, resp)
		case "write":
			resp := c.WriteToServer(op.Value, server.WritesFollowReads)
			log.Printf("[INFO] Client %d performed write operation with value %d: Response = %v", id, op.Value, resp)
		default:
			log.Printf("[WARN] Client %d encountered unknown operation type: %s", id, op.Type)
			continue
		}

		duration := time.Since(startOp)
		elapsedTime := time.Since(startTime).Seconds()

		metrics = append(metrics, Metric{
			OperationIndex: i + 1,
			OperationType:  op.Type,
			Latency:        duration.Seconds(),
			Timestamp:      elapsedTime,
		})

		if op.Delay > 0 {
			time.Sleep(time.Duration(op.Delay) * time.Millisecond)
		}
	}

	log.Printf("[INFO] Client %d completed workload", id)
	return metrics
}

func saveMetrics(metrics []Metric, filename string) {
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		log.Fatalf("[ERROR] Failed to serialize metrics: %v", err)
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Fatalf("[ERROR] Failed to write metrics to file: %v", err)
	}
	log.Printf("[INFO] Metrics saved to %s", filename)
}

func saveMetricsToCSV(metrics []Metric, latencyFile, throughputFile string) {
	// Save latency data
	latencyCSV, err := os.Create(latencyFile)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create latency CSV: %v", err)
	}
	defer latencyCSV.Close()
	latencyWriter := csv.NewWriter(latencyCSV)
	defer latencyWriter.Flush()

	latencyWriter.Write([]string{"OperationIndex", "Latency"})
	for _, metric := range metrics {
		latencyWriter.Write([]string{
			strconv.Itoa(metric.OperationIndex),
			strconv.FormatFloat(metric.Latency, 'f', 6, 64),
		})
	}

	// Save throughput data
	throughputCSV, err := os.Create(throughputFile)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create throughput CSV: %v", err)
	}
	defer throughputCSV.Close()
	throughputWriter := csv.NewWriter(throughputCSV)
	defer throughputWriter.Flush()

	throughputWriter.Write([]string{"Timestamp", "Throughput"})
	for _, metric := range metrics {
		throughput := float64(metric.OperationIndex) / metric.Timestamp
		throughputWriter.Write([]string{
			strconv.FormatFloat(metric.Timestamp, 'f', 6, 64),
			strconv.FormatFloat(throughput, 'f', 6, 64),
		})
	}

	log.Printf("[INFO] Latency data saved to %s", latencyFile)
	log.Printf("[INFO] Throughput data saved to %s", throughputFile)
}

func plotMetrics(metrics []Metric, latencyPlotFile, throughputPlotFile string) {
	// Plot latency
	latencyPlot := plot.New()
	latencyPlot.Title.Text = "Operation Latency"
	latencyPlot.X.Label.Text = "Operation Index"
	latencyPlot.Y.Label.Text = "Latency (s)"

	pts := make(plotter.XYs, len(metrics))
	for i, metric := range metrics {
		pts[i].X = float64(metric.OperationIndex)
		pts[i].Y = metric.Latency
	}

	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create latency plot: %v", err)
	}
	latencyPlot.Add(line)
	if err := latencyPlot.Save(8*vg.Inch, 4*vg.Inch, latencyPlotFile); err != nil {
		log.Fatalf("[ERROR] Failed to save latency plot: %v", err)
	}
	log.Printf("[INFO] Latency plot saved to %s", latencyPlotFile)

	// Plot throughput
	throughputPlot := plot.New()
	throughputPlot.Title.Text = "Throughput Over Time"
	throughputPlot.X.Label.Text = "Time (s)"
	throughputPlot.Y.Label.Text = "Throughput (ops/s)"

	throughputPts := make(plotter.XYs, len(metrics))
	for i, metric := range metrics {
		throughput := float64(metric.OperationIndex) / metric.Timestamp
		throughputPts[i].X = metric.Timestamp
		throughputPts[i].Y = throughput
	}

	line, err = plotter.NewLine(throughputPts)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create throughput plot: %v", err)
	}
	throughputPlot.Add(line)
	if err := throughputPlot.Save(8*vg.Inch, 4*vg.Inch, throughputPlotFile); err != nil {
		log.Fatalf("[ERROR] Failed to save throughput plot: %v", err)
	}
	log.Printf("[INFO] Throughput plot saved to %s", throughputPlotFile)
}
