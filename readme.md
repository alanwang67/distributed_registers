# Distributed Registers

- Install Nix and `direnv`, then run `direnv allow` || install Go 1.23.2
- Start server with `go run cmd/main.go server 0`, `go run cmd/main.go server 1`, etc.
- Start multiple clients with `go run cmd/main.go client 0`, `go run cmd/main.go client 1`, etc.

The client/server IDs are tied to the configs defined in `cmd/config.json`.
