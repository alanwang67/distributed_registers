package workload

import (
	"math/rand"
	"time"
)

// InstructionType constants define the types of operations.
type InstructionType string

const (
	InstructionTypeRead  InstructionType = "read"
	InstructionTypeWrite InstructionType = "write"
)

// Instruction represents a single operation in the workload.
type Instruction struct {
	Type  InstructionType `json:"type"`  // "read" or "write"
	Value uint64          `json:"value"` // Value to write (only used for write operations)
	Delay time.Duration   `json:"delay"` // Delay between instructions (in ms)
}

// ServerConfig represents a server configuration.
type ServerConfig struct {
	ID      uint64 `json:"id"`
	Network string `json:"network"`
	Address string `json:"address"`
}

// ClientConfig represents a client with a list of servers and its workload.
type ClientConfig struct {
	ID       uint64        `json:"id"`
	Servers  []int         `json:"servers"`
	Workload []Instruction `json:"workload"`
}

// Config represents the full configuration for servers and clients.
type Config struct {
	Servers []ServerConfig `json:"servers"`
	Clients []ClientConfig `json:"clients"`
}

// WorkloadGenerator generates workloads based on specified parameters.
type WorkloadGenerator struct {
	ReadPercentage   float64       // Percentage of read operations
	ZipfianS         float64       // S parameter for Zipfian distribution
	ZipfianV         uint64        // V parameter for Zipfian distribution
	OperationCount   int           // Total number of operations to generate
	MaxWriteValue    uint64        // Maximum value for write operations
	InstructionDelay time.Duration // Optional delay between instructions
	RNG              *rand.Rand    // Random generator for this workload
}

// NewWorkloadGenerator creates a new WorkloadGenerator with default parameters and a unique random seed.
func NewWorkloadGenerator(seed int64) *WorkloadGenerator {
	return &WorkloadGenerator{
		ReadPercentage:   0.8,
		ZipfianS:         1.01,
		ZipfianV:         1000000,
		OperationCount:   10, // Example workload size for simplicity
		MaxWriteValue:    1000000,
		InstructionDelay: 0,
		RNG:              rand.New(rand.NewSource(seed)),
	}
}

// Generate creates a workload based on the generator's parameters.
func (wg *WorkloadGenerator) Generate() []Instruction {
	zipf := rand.NewZipf(wg.RNG, wg.ZipfianS, 1, wg.ZipfianV)

	instructions := make([]Instruction, 0, wg.OperationCount)
	for i := 0; i < wg.OperationCount; i++ {
		var instrType InstructionType
		if wg.RNG.Float64() < wg.ReadPercentage {
			instrType = InstructionTypeRead
		} else {
			instrType = InstructionTypeWrite
		}

		value := zipf.Uint64() % wg.MaxWriteValue
		instr := Instruction{
			Type:  instrType,
			Value: value,
			Delay: wg.InstructionDelay,
		}
		instructions = append(instructions, instr)
	}
	return instructions
}

/*
// Main function to generate the `config.json` file.
func main() {
	// Define servers
	servers := []ServerConfig{
		{ID: 0, Network: "tcp", Address: "192.168.1.1:10000"},
		{ID: 1, Network: "tcp", Address: "192.168.1.2:10001"},
		{ID: 2, Network: "tcp", Address: "192.168.1.3:10002"},
	}

	// Define clients (only IDs, no hardcoded IPs)
	clients := []uint64{0, 1, 2}

	// Generate workloads for each client
	clientConfigs := []ClientConfig{}
	for _, clientID := range clients {
		// Use a unique random seed for each client
		seed := time.Now().UnixNano() + int64(clientID)
		generator := NewWorkloadGenerator(seed)

		// Generate a unique workload for the client
		workload := generator.Generate()

		clientConfigs = append(clientConfigs, ClientConfig{
			ID:       clientID,
			Servers:  []int{0, 1, 2}, // All clients connect to all servers
			Workload: workload,
		})
	}

	// Combine servers and clients into the final configuration
	config := Config{
		Servers: servers,
		Clients: clientConfigs,
	}

	// Convert the configuration to JSON
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Printf("Error generating config JSON: %v\n", err)
		return
	}

	// Save the configuration to a file
	outputFile := "config.json"
	err = os.WriteFile(outputFile, configJSON, 0644)
	if err != nil {
		fmt.Printf("Error writing config to file: %v\n", err)
		return
	}

	fmt.Printf("Configuration successfully written to %s\n", outputFile)
}
*/
