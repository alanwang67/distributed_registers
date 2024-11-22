package server

import (
    "testing"
)

func TestDependencyCheck(t *testing.T) {
    tests := []struct {
        name       string
        serverVC   []uint64
        requestWV  []uint64
        requestRV  []uint64
        session    SessionType
        expected   bool
    }{
        {
            name:       "Causal Dependency Satisfied",
            serverVC:   []uint64{2, 1, 0},
            requestWV:  []uint64{1, 1, 0},
            requestRV:  []uint64{2, 0, 0},
            session:    Causal,
            expected:   true,
        },
        {
            name:       "Causal Dependency Not Satisfied",
            serverVC:   []uint64{1, 1, 0},
            requestWV:  []uint64{2, 1, 0},
            requestRV:  []uint64{1, 0, 0},
            session:    Causal,
            expected:   false,
        },
        {
            name:       "Monotonic Reads Satisfied",
            serverVC:   []uint64{2, 2, 2},
            requestWV:  []uint64{1, 1, 1},
            requestRV:  nil,
            session:    MonotonicReads,
            expected:   true,
        },
        {
            name:       "Monotonic Writes Not Satisfied",
            serverVC:   []uint64{1, 1, 1},
            requestWV:  []uint64{2, 2, 2},
            requestRV:  nil,
            session:    MonotonicWrites,
            expected:   false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            serverData := Server{
                VectorClock: tt.serverVC,
            }
            request := ClientRequest{
                WriteVector: tt.requestWV,
                ReadVector:  tt.requestRV,
                SessionType: tt.session,
            }
            result := DependencyCheck(serverData, request)
            if result != tt.expected {
                t.Errorf("DependencyCheck(%v, %v) = %v; expected %v", serverData.VectorClock, request, result, tt.expected)
            }
        })
    }
}