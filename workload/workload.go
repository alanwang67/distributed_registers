package workload

import (
	"math/rand"
	"time"
)

// InstructionType constants define the types of operations.
const (
	InstructionTypeRead  = "read"
	InstructionTypeWrite = "write"
)

// Instruction represents a single operation in the workload.
type Instruction struct {
	Type  string        // "read" or "write"
	Value uint64        // Value to write (only used for write operations)
	Delay time.Duration // Optional delay before executing the instruction
}

// WorkloadGenerator generates workloads based on specified parameters.
type WorkloadGenerator struct {
	ReadPercentage   float64       // Percentage of read operations (e.g., 0.8 for 80% reads)
	ZipfianS         float64       // S parameter for Zipfian distribution (skewness)
	ZipfianV         uint64        // V parameter for Zipfian distribution (size of the keyspace)
	OperationCount   int           // Total number of operations to generate
	MaxWriteValue    uint64        // Maximum value for write operations
	InstructionDelay time.Duration // Optional delay between instructions
}

// NewWorkloadGenerator creates a new WorkloadGenerator with default parameters.
func NewWorkloadGenerator() *WorkloadGenerator {
	return &WorkloadGenerator{
		ReadPercentage:   0.8,     // Default to 80% reads
		ZipfianS:         1.01,    // Skewness parameter
		ZipfianV:         1000000, // Size of the keyspace
		OperationCount:   1000,    // Total operations
		MaxWriteValue:    1000000, // Maximum value for writes
		InstructionDelay: 0,       // No delay between instructions by default
	}
}

// Generate creates a workload based on the generator's parameters.
func (wg *WorkloadGenerator) Generate() []Instruction {
	rand.Seed(time.Now().UnixNano())
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), wg.ZipfianS, 1, wg.ZipfianV)

	instructions := make([]Instruction, 0, wg.OperationCount)

	for i := 0; i < wg.OperationCount; i++ {
		// Determine if this is a read or write operation
		var instrType string
		if rand.Float64() < wg.ReadPercentage {
			instrType = InstructionTypeRead
		} else {
			instrType = InstructionTypeWrite
		}

		// Generate a value based on Zipfian distribution
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
// Main function for generating a workload and saving it as a JSON file
func main() {
	// Create a workload generator with default parameters
	generator := NewWorkloadGenerator()

	// Generate the workload
	workload := generator.Generate()

	// Convert the workload to JSON
	workloadJSON, err := json.MarshalIndent(workload, "", "  ")
	if err != nil {
		fmt.Printf("Error generating workload JSON: %v\n", err)
		return
	}

	// Save the workload to a file
	outputFile := "workload.json"
	err = os.WriteFile(outputFile, workloadJSON, 0644)
	if err != nil {
		fmt.Printf("Error writing workload to file: %v\n", err)
		return
	}

	fmt.Printf("Workload successfully written to %s\n", outputFile)
}
*/
