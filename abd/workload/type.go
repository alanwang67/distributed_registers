package workload

import (
	"time"
)

type Instruction struct {
	Type  string        // Type of instruction: read, write
	Value uint64        // Value to write
	Delay time.Duration // Optional delay between operations
}
