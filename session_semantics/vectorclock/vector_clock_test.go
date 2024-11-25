package vectorclock

import (
	"testing"
)

func TestCompareVersionVector(t *testing.T) {
	tests := []struct {
		v1     []uint64
		v2     []uint64
		expect bool
	}{
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3}, true},  // v1 == v2
		{[]uint64{2, 3, 4}, []uint64{1, 2, 3}, true},  // v1 > v2
		{[]uint64{1, 1, 1}, []uint64{2, 2, 2}, false}, // v1 < v2
		{[]uint64{3, 2, 1}, []uint64{1, 2, 3}, false}, // Mixed domination
		{[]uint64{}, []uint64{}, true},                // Empty vectors
		{[]uint64{1}, []uint64{0}, true},              // Single element, v1 > v2
		{[]uint64{0}, []uint64{1}, false},             // Single element, v1 < v2
	}

	for _, tt := range tests {
		result := CompareVersionVector(tt.v1, tt.v2)
		if result != tt.expect {
			t.Errorf("CompareVersionVector(%v, %v) = %v; want %v", tt.v1, tt.v2, result, tt.expect)
		}
	}
}

func TestGetMaxVersionVector(t *testing.T) {
	tests := []struct {
		lst    [][]uint64
		expect []uint64
	}{
		{[][]uint64{{1, 2, 3}, {2, 1, 4}, {0, 3, 2}}, []uint64{2, 3, 4}},  // General case
		{[][]uint64{{1, 2}, {2, 3}, {3, 1}}, []uint64{3, 3}},              // Two-element vectors
		{[][]uint64{}, nil},                                              // Empty list
		{[][]uint64{{1, 2, 3}}, []uint64{1, 2, 3}},                       // Single vector
		{[][]uint64{{5, 5, 5}, {0, 0, 0}}, []uint64{5, 5, 5}},            // Dominance by first
		{[][]uint64{{0, 0, 0}, {5, 5, 5}}, []uint64{5, 5, 5}},            // Dominance by last
	}

	for _, tt := range tests {
		result := GetMaxVersionVector(tt.lst)
		if len(result) != len(tt.expect) || !compareSlices(result, tt.expect) {
			t.Errorf("GetMaxVersionVector(%v) = %v; want %v", tt.lst, result, tt.expect)
		}
	}
}

func TestConcurrentVersionVectors(t *testing.T) {
	tests := []struct {
		v1     []uint64
		v2     []uint64
		expect bool
	}{
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3}, false}, // Identical vectors
		{[]uint64{2, 3, 4}, []uint64{1, 2, 3}, false}, // v1 > v2
		{[]uint64{1, 1, 1}, []uint64{2, 2, 2}, false}, // v1 < v2
		{[]uint64{1, 3, 2}, []uint64{2, 1, 3}, true},  // Concurrent
		{[]uint64{}, []uint64{}, false},               // Empty vectors
	}

	for _, tt := range tests {
		result := ConcurrentVersionVectors(tt.v1, tt.v2)
		if result != tt.expect {
			t.Errorf("ConcurrentVersionVectors(%v, %v) = %v; want %v", tt.v1, tt.v2, result, tt.expect)
		}
	}
}

// Helper function to compare two slices
func compareSlices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}