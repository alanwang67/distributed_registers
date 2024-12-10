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

	for _, server := range c.Servers {
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Failed to connect to server %v: %v", server, err)
			continue
		}

		request := map[string]interface{}{"type": "read"}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Failed to send read request to server %v: %v", server, err)
			conn.Close()
			continue
		}

		var response map[string]interface{}
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			log.Printf("Failed to decode read response from server %v: %v", server, err)
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

	if responses < quorum {
		log.Printf("Read failed: insufficient responses to achieve quorum.")
		return latestValue, maxVersion
	}

	log.Printf("Read successful: Value=%d, Version=%d", latestValue, maxVersion)
	return latestValue, maxVersion
}

// Write performs the ABD write operation in two phases:
// 1. Fetch the current state (optional for generating unique version numbers).
// 2. Broadcast the new (value, version) pair to all servers.
func (c *Client) Write(value int) (bool, int) {
	quorum := len(c.Servers)/2 + 1
	maxVersion := 0
	responses := 0

	// Phase 1: Fetch current version from servers
	for _, server := range c.Servers {
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Failed to connect to server %v: %v", server, err)
			continue
		}

		request := map[string]interface{}{"type": "read"}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Failed to send read request to server %v: %v", server, err)
			conn.Close()
			continue
		}

		var response map[string]interface{}
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			log.Printf("Failed to decode read response from server %v: %v", server, err)
			conn.Close()
			continue
		}

		conn.Close()

		version := int(response["version"].(float64))
		if version > maxVersion {
			maxVersion = version
		}
		responses++
	}

	if responses < quorum {
		log.Printf("Write aborted: insufficient responses during version fetch.")
		return false, maxVersion
	}

	// Phase 2: Write the new value with incremented version
	successfulWrites := 0
	newVersion := maxVersion + 1

	for _, server := range c.Servers {
		conn, err := net.Dial("tcp", server["address"].(string))
		if err != nil {
			log.Printf("Failed to connect to server %v: %v", server, err)
			continue
		}

		request := map[string]interface{}{
			"type":    "write",
			"value":   value,
			"version": newVersion,
		}

		if err := json.NewEncoder(conn).Encode(request); err != nil {
			log.Printf("Failed to send write request to server %v: %v", server, err)
			conn.Close()
			continue
		}

		var response map[string]interface{}
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			log.Printf("Failed to decode response from server %v: %v", server, err)
			conn.Close()
			continue
		}

		conn.Close()

		if response["status"] == "ok" {
			successfulWrites++
		}
	}

	if successfulWrites >= quorum {
		log.Printf("Write successful: Value=%d, Version=%d", value, newVersion)
		return true, newVersion
	}

	log.Printf("Write failed to achieve quorum: Value=%d, Version=%d", value, maxVersion)
	return false, maxVersion
}
