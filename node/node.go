package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"time"

	pb "4435Asn3/proto"

	"github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Node type for storing information about this server/client
type node struct {
	// Self information
	Name string
	Addr string

	// Consul related variables
	SDAddress string
	SDKV      api.KV

	// used to make requests
	Clients map[string]pb.ConsistentHashClient
	// track the number of clients
	NumClients int

	RingClients []string
	// ring size
	ringSize int

	// finger table size
	fingerTableSize int
	fingerTable [] fingerTableEntry
}

type fingerTableEntry struct{
	i int
	id2i int
	succ int
}


/* RPC functions */
// Test connection with a client
func (n *node) TestConnection(ctx context.Context, in *pb.TestRequest) (*pb.GenericResponse, error) {
	return &pb.GenericResponse{Message: "Successfully connected..."}, nil
}

// Compute the finger table for a node
func (n *node) ComputeFingerTable(ctx context.Context, in *pb.Empty) (*pb.GenericResponse, error) {
	return &pb.GenericResponse{Message: "Successfully connected..."}, nil
}

// Compute location for string on the ring
func (n *node) HashString(value string) int {
	h := fnv.New32a()
	h.Write([]byte(value))
	return int(math.Mod(float64(h.Sum32()), float64(n.ringSize)))
}

// Main method of server
func main() {
	// pass the port as an argument and also the port of the other node
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Arguments required: <node name> <port> <consul address>")
		return
	}
	node_name := args[0]
	port := args[1]
	sdaddress := args[2]

	// Stores all information about the server in a struct


	noden := node{Name: node_name, Addr: port, SDAddress: sdaddress, Clients: nil, RingClients: nil, ringSize: (2 ^ 10) - 1, fingerTableSize: 10, fingerTable: nil}

	// start the node
	noden.Start()
}

/* Setup functions from sample code */
// Start the node.
// This starts listening at the configured address. It also sets up clients for it's peers.
func (n *node) Start() {
	// init required variables
	n.Clients = make(map[string]pb.ConsistentHashClient)

	// start service / listening
	go n.StartListening()

	// register with the service discovery unit
	n.registerService()

	//Hash this nodes own id
	ringId := n.HashString(n.Name)
	n.RingClients = make([]string, n.ringSize)
	n.RingClients[ringId] = n.Name
	n.fingerTable = make([]fingerTableEntry, n.fingerTableSize)


	// connect to all other nodes on the consul
	for{
		fmt.Println("...")
		time.Sleep(5 * time.Second)

		prevNumClients := n.NumClients
		n.ConnectToAllNodes()

		fmt.Println("NUMBER OF NODES ON THE NETWORK: ", n.NumClients)
		time.Sleep(3*time.Second)

		if (prevNumClients==n.NumClients){ continue }

		fmt.Println("recomputing finger tables")
		//num clients has changed, recompute finger tables
		for index := 0; index<n.fingerTableSize; index++{
			id2index := ringId + (2^index)
			successor := n.findSuccessorOf(id2index)
			n.fingerTable[index] = fingerTableEntry{i: index, id2i: id2index, succ: successor}
			
		}
		fmt.Println("recomputed.")
		//HANDLE
	}
	
}
func (n *node) findSuccessorOf(id int) (successor int){
	
	prevRingID:= -1
	for ringID, _ := range n.RingClients {
		if(ringID > id){
			if (prevRingID==-1){ 
				prevRingID=ringID 
				break
			}
			return prevRingID
		}
		prevRingID=ringID
	}

	for ringID, _ := range n.RingClients{
		if(ringID > id){
			return prevRingID
		}
		prevRingID=ringID
	}

	return -1
}

// Register self with the service discovery module.
// This implementation simply uses the key-value store. One major drawback is that when nodes crash. nothing is updated on the key-value store. Services are a better fit and should be used eventually.
func (n *node) registerService() {
	config := api.DefaultConfig()
	config.Address = n.SDAddress
	consul, err := api.NewClient(config)
	if err != nil {
		log.Panicln("Unable to contact Service Discovery.")
	}

	kv := consul.KV()
	p := &api.KVPair{Key: n.Name, Value: []byte(n.Addr)}
	_, err = kv.Put(p, nil)
	if err != nil {
		log.Panicln("Unable to register with Service Discovery.")
	}

	// store the kv for future use
	n.SDKV = *kv

	log.Println("Successfully registered with Consul.")
}

// Start listening/service.
func (n *node) StartListening() {

	lis, err := net.Listen("tcp", n.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	_n := grpc.NewServer() // n is for serving purpose

	pb.RegisterConsistentHashServer(_n, n)
	// Register reflection service on gRPC server.
	reflection.Register(_n)

	// start listening
	if err := _n.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Busy Work module, greet every new member you find
func (n *node) ConnectToAllNodes() {
	// Get all nodes registered to the consul (with csuthe as the name)
	kvpairs, _, err := n.SDKV.List("jmaxwe29", nil)
	if err != nil {
		log.Panicln(err)
		return
	}

	for _, kventry := range kvpairs {
		if strings.Compare(kventry.Key, n.Name) == 0 {
			continue
		}
		if n.Clients[kventry.Key] == nil {
			fmt.Println("Setting up node ", kventry.Key)
			n.SetupClient(kventry.Key, string(kventry.Value))
		}
	}
	n.NumClients = len(n.Clients)

	// Go through all other nodes on the network and make them aware that this node was added
	// for _, client := range n.Clients {
	//     client.
	// }
}

// Setup a new grpc client for contacting the server at addr.
func (n *node) SetupClient(name string, addr string) {

	// setup connection with other node
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	fmt.Println(name)
	n.Clients[name] = pb.NewConsistentHashClient(conn)
	n.RingClients[n.HashString(name)] = name
}
