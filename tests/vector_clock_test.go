package server

import (
	"testing"
)

func TestCompareVersionVector(t *testing.T) {
    tests := []struct {
        name     string
        v1       []uint64
        v2       []uint64
        expected bool
    }{
        {"Equal Vectors", []uint64{1, 2, 3}, []uint64{1, 2, 3}, true},
        {"v1 Greater Than v2", []uint64{2, 3, 4}, []uint64{1, 2, 3}, true},
        {"v1 Less Than v2", []uint64{1, 2, 2}, []uint64{1, 2, 3}, false},
        {"v1 and v2 Concurrent", []uint64{2, 1, 3}, []uint64{1, 2, 3}, false},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := compareVersionVector(tt.v1, tt.v2)
            if result != tt.expected {
                t.Errorf("compareVersionVector(%v, %v) = %v; expected %v", tt.v1, tt.v2, result, tt.expected)
            }
        })
    }
}