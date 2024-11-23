package vectorclock

type VectorClock struct {
	Clock 	[]uint64
}

// Compare returns true if v1 dominates v2 element-wise.
func CompareVersionVector(v1 []uint64, v2 []uint64) bool {
	for i := uint64(0); i < uint64(len(v1)); i++ {
		if v1[i] < v2[i] {
			return false
		}
	}
	return true
}

// GetMax returns a new vector clock where each element is the maximum of the corresponding elements in the input vectors.
func GetMaxVersionVector(lst [][]uint64) []uint64 {
    if len(lst) == 0 {
        return nil
    }
    // Initialize mx as a copy of the first vector to avoid modifying it
    mx := make([]uint64, len(lst[0]))
    copy(mx, lst[0])
    for i := 1; i < len(lst); i++ {
        for j := 0; j < len(lst[i]); j++ {
            if lst[i][j] > mx[j] {
                mx[j] = lst[i][j]
            }
        }
    }
    return mx
}

// Concurrent returns true if v1 and v2 are concurrent (neither vector dominates the other).
func ConcurrentVersionVectors(v1 []uint64, v2 []uint64) bool {
	return !CompareVersionVector(v1, v2) && !CompareVersionVector(v2, v1)
}