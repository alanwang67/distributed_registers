package client

import (
	"encoding/json"
	"log"
	"net"
)

// Client represents a single client in the distributed system.
// Each client communicates with a set of servers to perform read and write operations
// following the ABD algorithm for quorum-based consistency.
type Client struct {
	ID      int                      // Unique ID of the client
	Servers []map[string]interface{} // List of server configurations
}

// Read performs the ABD read operation in two phases:
// 1. Get Phase: Contacts all servers to fetch the highest version and value.
// 2. Set Phase: Writes back the highest version and value to all servers to ensure atomicity.
func (c *Client) Read() (int, int) {
	maxVersion := 0
	var latestValue int
	quorum := len(c.Servers)/2 + 1
	responses := 0

	// Phase 1: Get phase - Fetch value and version from all servers
	for _, server := range c.Servers {
		log.Printf("Client %d fetching current state from server %v", c.ID, server)
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Client %d failed to connect to server %v: %v", c.ID, server, err)
			continue
		}

		request := map[string]interface{}{"type": "read"}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Client %d failed to send read request to server %v: %v", c.ID, server, err)
			conn.Close()
			continue
		}

		var response map[string]interface{}
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			log.Printf("Client %d failed to decode read response from server %v: %v", c.ID, server, err)
			conn.Close()
			continue
		}
		conn.Close()

		version := int(response["version"].(float64))
		value := int(response["value"].(float64))
		if version > maxVersion {
			maxVersion = version
			latestValue = value
		}
		responses++
	}

	// Ensure quorum was reached
	if responses < quorum {
		log.Printf("Client %d could not achieve quorum for read operation", c.ID)
		return latestValue, maxVersion
	}

	// Phase 2: Write-back phase - Broadcast the latest value and version to all servers
	for _, server := range c.Servers {
		log.Printf("Client %d attempting to set phase on server %v", c.ID, server)
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Client %d failed to connect to server %v: %v", c.ID, server, err)
			continue
		}

		request := map[string]interface{}{
			"type":    "write",
			"value":   latestValue,
			"version": maxVersion,
		}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Client %d failed to send set phase to server %v: %v", c.ID, server, err)
			conn.Close()
			continue
		}
		conn.Close()
		log.Printf("Client %d completed set phase on server %v", c.ID, server)
	}

	return latestValue, maxVersion
}

// Write performs the ABD write operation in two phases:
// 1. Fetch the current state (optional for generating unique version numbers).
// 2. Broadcast the new (value, version) pair to all servers.
func (c *Client) Write(value int, version int) {
	responses := 0
	quorum := len(c.Servers)/2 + 1

	// Broadcast new (value, version) to all servers
	for _, server := range c.Servers {
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Client %d failed to connect to server %v: %v", c.ID, server, err)
			continue
		}

		// Send write request
		request := map[string]interface{}{
			"type":    "write",
			"value":   value,
			"version": version,
		}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Client %d failed to send write request to server %v: %v", c.ID, server, err)
			conn.Close()
			continue
		}

		conn.Close()
		responses++
	}

	// Ensure quorum was reached
	if responses < quorum {
		log.Printf("Client %d failed to achieve quorum for write operation", c.ID)
	} else {
		log.Printf("Client %d successfully wrote value %d with version %d to quorum", c.ID, value, version)
	}
}
