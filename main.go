package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"ricart-agrawala/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// State defines the three possible states a node can be in regarding the Critical Section (CS)
type State int

const (
	RELEASED State = iota // Node does not want the CS
	WANTED                // Node wants the CS and is waiting for replies
	HELD                  // Node is currently executing inside the CS
)

// Node represents a single machine in our distributed system
type Node struct {
	pb.UnimplementedMutexServiceServer
	mu sync.Mutex // Mutex to prevent race conditions on the node's local variables

	id    int32    // Unique identifier for this node (1, 2, or 3)
	port  string   // The port this node listens on
	peers []string // The network addresses of the other nodes in the system

	state    State // Current state of the node (RELEASED, WANTED, HELD)
	clock    int32 // Lamport logical clock to track events
	reqClock int32 // The exact timestamp when this node last requested the CS

	// A list of channels. When we defer a peer's request, we create a channel to "freeze"
	// their reply. When we exit the CS, we send a signal through these channels to unfreeze them.
	deferred []chan bool
}

// RequestAccess is the gRPC method triggered by OTHER nodes when they want to enter the CS.
func (n *Node) RequestAccess(ctx context.Context, req *pb.AccessRequest) (*pb.AccessReply, error) {
	n.mu.Lock()

	// Update local Lamport clock based on the incoming request's timestamp.
	// We take the max of our clock and the request's clock, then increment.
	if req.Timestamp > n.clock {
		n.clock = req.Timestamp
	}
	n.clock++

	log.Printf("[RECEIVE] Received Request from Node %d with Timestamp %d", req.NodeId, req.Timestamp)

	// Ricart-Agrawala Deferral Logic:
	// We must defer (delay our reply to) the requesting node IF:
	// 1. We are currently IN the Critical Section (HELD)
	// 2. We WANT the Critical Section (WANTED) AND we have priority.
	// Priority means we have a lower timestamp. If timestamps are tied, lower Node ID wins.
	deferRequest := false
	if n.state == HELD {
		deferRequest = true
	} else if n.state == WANTED {
		// Tie-breaker logic: lower timestamp wins. If tied, lower node ID wins.
		if (n.reqClock < req.Timestamp) || (n.reqClock == req.Timestamp && n.id < req.NodeId) {
			deferRequest = true
		}
	}

	var waitChan chan bool
	if deferRequest {
		log.Printf("[DEFER] Deferring request from Node %d", req.NodeId)
		waitChan = make(chan bool)
		n.deferred = append(n.deferred, waitChan) // Add to our defer queue
	}

	// We MUST unlock here. If we don't, the node freezes entirely and can't do anything else
	// (like exiting the CS) while it waits to reply.
	n.mu.Unlock()

	// If we decided to defer the request, this line blocks the function from returning.
	// It waits here until ExitCriticalSection() sends a signal into waitChan.
	if deferRequest {
		<-waitChan
	}

	// Once unblocked (or if we never deferred), we reply granting permission.
	return &pb.AccessReply{Granted: true}, nil
}

// EnterCriticalSection initiates the process of asking peers for permission to enter the CS
func (n *Node) EnterCriticalSection() {
	n.mu.Lock()
	n.state = WANTED     // Update state
	n.clock++            // Increment clock for this new event
	n.reqClock = n.clock // Save the timestamp for this specific request
	currentClock := n.reqClock
	n.mu.Unlock()

	log.Printf("[WANTED] Node %d wants to enter CS at timestamp %d", n.id, currentClock)

	// We use a WaitGroup to wait until we get a reply from ALL peers (N-1 replies)
	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			// Connect to the peer via gRPC
			conn, err := grpc.Dial(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Failed to connect to peer %s: %v", p, err)
			}
			defer conn.Close()

			client := pb.NewMutexServiceClient(conn)
			log.Printf("[SEND] Sending Request to peer %s", p)

			// Send the request. This will block if the peer decides to defer us.
			_, err = client.RequestAccess(context.Background(), &pb.AccessRequest{
				NodeId:    n.id,
				Timestamp: currentClock,
			})
			if err != nil {
				log.Printf("Failed to get reply from %s: %v", p, err)
			} else {
				log.Printf("[REPLY] Received granted reply from %s", p)
			}
		}(peer)
	}

	// Wait here until ALL peers have replied "Granted"
	wg.Wait()

	// Once we have all replies, we safely enter the Critical Section (Requirement R2: Safety)
	n.mu.Lock()
	n.state = HELD
	n.mu.Unlock()

	// --- SYSTEM REQUIREMENT R1: EMULATING THE CRITICAL SECTION ---
	log.Printf("==== NODE %d ENTERED CRITICAL SECTION ====", n.id)
	time.Sleep(2 * time.Second) // Emulate sensitive operation taking time
	log.Printf("==== NODE %d LEAVING CRITICAL SECTION ====", n.id)
	// -------------------------------------------------------------

	n.ExitCriticalSection()
}

// ExitCriticalSection cleans up after the node finishes the sensitive operation
func (n *Node) ExitCriticalSection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// We no longer need the CS
	n.state = RELEASED
	log.Printf("[RELEASED] Node %d exited CS", n.id)

	// Requirement R3: Liveliness. We must unblock any nodes we deferred so they can proceed.
	// We do this by sending a "true" signal into the channels that are blocked in RequestAccess.
	for _, ch := range n.deferred {
		ch <- true
	}
	// Clear the queue since everyone has been unblocked
	n.deferred = nil
}

func main() {
	// Parse the Node ID from the command line (e.g., -id 1)
	id := flag.Int("id", 1, "Node ID (1, 2, or 3)")
	flag.Parse()

	var port string
	var peers []string

	// --- HARDCODED SERVICE DISCOVERY ---
	// Based on the given Node ID, we hardcode its listening port and who its peers are.
	switch *id {
	case 1:
		port = ":5001"
		peers = []string{"localhost:5002", "localhost:5003"}
	case 2:
		port = ":5002"
		peers = []string{"localhost:5001", "localhost:5003"}
	case 3:
		port = ":5003"
		peers = []string{"localhost:5001", "localhost:5002"}
	default:
		log.Fatalf("Unknown node ID: %d. Please use 1, 2, or 3", *id)
	}

	// Initialize our node struct
	node := &Node{
		id:       int32(*id),
		port:     port,
		peers:    peers,
		state:    RELEASED,
		clock:    0,
		deferred: make([]chan bool, 0), // Initialize the empty deferral queue
	}

	// Start the gRPC server to listen for incoming requests from peers
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMutexServiceServer(grpcServer, node)

	// Run the server in a background goroutine so it doesn't block main()
	go func() {
		log.Printf("Node %d listening on %s", node.id, port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait 10 seconds to give the user enough time to manually start all 3 terminal windows.
	// This ensures no "connection refused" errors happen before the algorithm begins.
	time.Sleep(10 * time.Second)

	// Add a slight random delay (0-2 seconds) so nodes don't all request at the exact same millisecond.
	// This helps create overlaps to prove the Ricart-Agrawala algorithm resolves conflicts.
	rand.Seed(time.Now().UnixNano() + int64(*id))
	time.Sleep(time.Duration(rand.Intn(3)) * time.Second)

	// Start the process to request the Critical Section
	node.EnterCriticalSection()

	// Keep the program alive indefinitely so it can continue answering peer requests
	select {}
}
