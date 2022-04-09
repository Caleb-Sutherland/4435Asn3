package main

import (
	"fmt"
	"io"

	//"hash/fnv"
	"bytes"
	"errors"
	"log"
	"math"
	"net"
	"os"
	"strconv"
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

	// ring size
	ringSize int

	// this nodes id in the ring
	ringId int

	// Pointers to successor and predecessor if this node
	successor   *neighbour
	predecessor *neighbour

	// Directory to store this nodes files
	storage string
}

// used for successors and predecessors
type neighbour struct {
	position int
	client   pb.ConsistentHashClient
}

/* RPC functions */
// Test connection with a client
func (n *node) TestConnection(ctx context.Context, in *pb.TestRequest) (*pb.GenericResponse, error) {
	return &pb.GenericResponse{Message: "Successfully connected..."}, nil
}

// Checks if a newly added node is the successor or predecessor of this node, returns if it is
func (n *node) NotifyNewNodeAdded(ctx context.Context, in *pb.NodeInfo) (*pb.NodeInfoResponse, error) {
	// If the newly joined node conflicts with another nodes position, indicate for it to shutdown
	// We are checking if this new node hashed to the same position as this node, its successor, or its predecessor
	if in.Position == int64(n.ringId) || (n.successor != nil && int64(n.successor.position) == in.Position) || (n.predecessor != nil && int64(n.predecessor.position) == in.Position) {
		return &pb.NodeInfoResponse{IsSucc: false, IsPred: false, Position: int64(n.ringId)}, errors.New("node conflicts with another node on the ring")
	}

	// takes in position of the new node added to the ring
	response := &pb.NodeInfoResponse{IsSucc: false, IsPred: false, Position: int64(n.ringId)}

	// if the new node is the successor, must set the pointer correctly, return response indicating that this is node is a predecessor
	if n.successor == nil || n.isNewSuccessor(int(in.Position)) {
		conn := GetConnection(in.Address)
		n.successor = &neighbour{position: int(in.Position), client: conn}
		response.IsPred = true
		fmt.Println("Successor set to node at position " + strconv.Itoa(int(in.Position)) + "...")
	}

	// if the new node is the predecessor, must set the pointer correctly, return response indicating that this is node is a successor
	if n.predecessor == nil || n.isNewPredecessor(int(in.Position)) {
		conn := GetConnection(in.Address)
		n.predecessor = &neighbour{position: int(in.Position), client: conn}
		response.IsSucc = true
		fmt.Println("Predecessor set to node at position " + strconv.Itoa(int(in.Position)) + "...")
	}

	fmt.Print("\n")
	return response, nil
}

// Helper function to determine if a new node is the new successor of this node
func (n *node) isNewSuccessor(pos int) bool {
	localPos := n.ringId
	succPos := n.successor.position

	// Common situation where the successor has a larger position than the current node
	if succPos > localPos && pos < succPos && pos > localPos {
		return true
	}

	// Other situation where successor is at beginning of the ring, so its position is technically smaller
	if succPos < localPos && ((pos > localPos && pos < n.ringSize) || pos < succPos && pos >= 0) {
		return true
	}

	return false
}

// Helper function to determine if a new node is the new predecessor of this node
func (n *node) isNewPredecessor(pos int) bool {
	localPos := n.ringId
	predPos := n.predecessor.position

	// Common situation where the current node has a larger position than the predecessor node
	if localPos > predPos && pos < localPos && pos > predPos {
		return true
	}

	// Other situation where predeccesor is at beginning of the ring, so its position is technically larger than this node
	if localPos < predPos && ((pos > predPos && pos < n.ringSize) || pos < localPos && pos >= 0) {
		return true
	}

	return false
}

// Add a file to the network
func (n *node) AddFile(stream pb.ConsistentHash_AddFileServer) error {
	// Recieve the file name
	req, err := stream.Recv()
	if err != nil {
		fmt.Println("Could not recieve the file upload")
		return errors.New("failed to upload the file")
	}
	fileData := bytes.Buffer{}
	filename := req.GetFilename()
	fmt.Println("RECIEVING: " + filename)

	// Recive the rest of the file
	for {
		fmt.Println("waiting to recieve more data...")
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("End of file reached!")
			break
		}

		if err != nil {
			return errors.New("error uploading file")
		}

		chunk := req.GetChunkData()
		_, err = fileData.Write((chunk))
		if err != nil {
			return errors.New("error writing the file to buffer")
		}

		fmt.Println(string(chunk))
	}

	err = stream.SendAndClose(&pb.UploadStatus{Message: "Closing the stream"})
	if err != nil {
		return errors.New("error closing the stream")
	}

	fmt.Println(filename + " successfully uploaded!")
	return nil
}

// Helper function to get a connection given an address
func GetConnection(address string) pb.ConsistentHashClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return pb.NewConsistentHashClient(conn)
}

// Compute location for string on the ring
func (n *node) HashString(value string) int {
	// h := fnv.New32a()
	// h.Write([]byte(value))
	// return int(math.Mod(float64(h.Sum32()), float64(n.ringSize)))

	// Temporarily just place the node in the position specified at end of name: e.g. csuthe 30 will go at position 30
	inputList := strings.Fields(value)
	pos, _ := strconv.Atoi(inputList[1])
	return int(math.Mod(float64(pos), float64(10)))
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
	noden := node{Name: node_name, Addr: port, SDAddress: sdaddress, ringSize: 10, ringId: -1, storage: node_name}

	// start the node
	noden.Start()
}

// This starts the node listening at the configured address. It also configures the network to handle a newly added node
func (n *node) Start() {
	// start service / listening
	go n.StartListening()

	// register with the service discovery unit
	n.registerService()

	//Hash this nodes own id
	n.ringId = n.HashString(n.Name)

	// Configure the network to handle this newly started node
	err := n.ConfigureNetworkForNewNode()
	if err != nil {
		fmt.Println(err)
	}

	// Loop to recieve messages
	for {
		// fmt.Println("waiting for message...")
		time.Sleep(1 * time.Second)
	}
}

// Register a node to the consul so that it can be accessed when a new node join the network
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

// Inform all nodes on the network that this node has joined, also compute the successor and predecessor
func (n *node) ConfigureNetworkForNewNode() error {
	// Get all nodes registered to the consul (with csuthe as the name)
	kvpairs, _, err := n.SDKV.List("csuthe", nil)
	if err != nil {
		log.Panicln(err)
		return nil
	}

	// Notify every node that this node has joined the network, during this exchange, calculate who the successor and predecessor are
	for _, kventry := range kvpairs {
		if strings.Compare(kventry.Key, n.Name) == 0 {
			continue
		}

		fmt.Println("Sending notification to " + kventry.Key)
		currClient := GetConnection(string(kventry.Value))
		response, err := currClient.NotifyNewNodeAdded(context.Background(), &pb.NodeInfo{Position: int64(n.ringId), Address: n.Addr})

		// If there was an error while notifying other nodes, shut down this node
		if err != nil {
			return err
		}

		// If the node contacted is the sucessor of this node, store it
		if response.IsSucc {
			n.successor = &neighbour{position: int(response.Position), client: currClient}
			fmt.Println("Successor set to node at position " + strconv.Itoa(n.successor.position) + " of the ring...")
		}

		// If the node contacted is the predecessor of this node, store it
		if response.IsPred {
			n.predecessor = &neighbour{position: int(response.Position), client: currClient}
			fmt.Println("Predecessor set to node at position " + strconv.Itoa(n.predecessor.position) + " of the ring...")
		}
	}
	return nil
}

