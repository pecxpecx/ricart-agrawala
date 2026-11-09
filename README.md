# Distributed Mutual Exclusion (Ricart-Agrawala)

This project implements distributed mutual exclusion using the Ricart-Agrawala algorithm in Golang via gRPC. 

It simulates three peer nodes that request and grant access to a Critical Section using logical clocks and a deferral mechanism.

## Service Discovery
Service discovery is implemented using the hardcoding method. Each node's IP, port, and peer list are defined internally, and the correct configuration is loaded simply by passing a Node ID (1, 2, or 3) via the command line at startup.

## Prerequisites / Requirements

Before running the system, ensure your environment is set up with the following:

1. **Go (Golang):** You must have Go installed on your system. You can verify this by running `go version` in your terminal.
2. **Project Dependencies:** The project relies on the Go gRPC and Protobuf libraries. Before running the nodes, navigate to the project root and download the dependencies by running:
   ```bash
   go mod tidy

## How To Run
Open three separate terminal windows and navigate to the project directory in all of them.

**In Terminal 1**, start Node 1:
go run main.go -id 1
   
**In Terminal 2**, start Node 2:
go run main.go -id 2

**In Terminal 3**, start Node 3:
go run main.go -id 3
