package main

import (
	"bufio"
	"fmt"
	"io"

	//"hash/fnv"
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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
func (n *node) UpdateSuccessor(ctx context.Context, in *pb.UpdateNeighbourRequest) (*pb.UpdateNeighbourResponse, error) {
	if in.Position == -1 {
		n.successor = nil
		fmt.Println("Successor set to nil...")
		return &pb.UpdateNeighbourResponse{Message: "Success"}, nil
	}

	conn := GetConnection(in.Address)
	n.successor = &neighbour{position: int(in.Position), client: conn}
	fmt.Println("Successor set to node at position " + strconv.Itoa(int(in.Position)) + "...")
	return &pb.UpdateNeighbourResponse{Message: "Success"}, nil
}

func (n *node) UpdatePredecessor(ctx context.Context, in *pb.UpdateNeighbourRequest) (*pb.UpdateNeighbourResponse, error) {
	if in.Position == -1 {
		n.predecessor = nil
		fmt.Println("Predecessor set to nil...")
		return &pb.UpdateNeighbourResponse{Message: "Success"}, nil
	}

	conn := GetConnection(in.Address)
	n.predecessor = &neighbour{position: int(in.Position), client: conn}
	fmt.Println("Predecessor set to node at position " + strconv.Itoa(int(in.Position)) + "...")
	return &pb.UpdateNeighbourResponse{Message: "Success"}, nil
}

func (n *node) GetNodeInfo(ctx context.Context, in *pb.Empty) (*pb.NodeInfo, error) {
	return &pb.NodeInfo{Address: n.Addr, Position: int64(n.ringId)}, nil
}

// Test connection with a client
func (n *node) TestConnection(ctx context.Context, in *pb.TestRequest) (*pb.GenericResponse, error) {
	return &pb.GenericResponse{Message: "Successfully connected to node " + n.Name + "..."}, nil
}

// Checks if a newly added node is the successor or predecessor of this node, returns if it is
func (n *node) NotifyNewNodeAdded(ctx context.Context, in *pb.NodeInfo) (*pb.NodeInfoResponse, error) {
	// If the newly joined node conflicts with another nodes position, indicate for it to shutdown
	// We are checking if this new node hashed to the same position as this node, its successor, or its predecessor
	if in.Position == int64(n.ringId) || (n.successor != nil && int64(n.successor.position) == in.Position) || (n.predecessor != nil && int64(n.predecessor.position) == in.Position) {
		return &pb.NodeInfoResponse{IsSucc: false, IsPred: false, Position: int64(n.ringId)}, errors.New("node conflicts with another nodes position on the ring. please pick a different node name")
	}

	// takes in position of the new node added to the ring
	response := &pb.NodeInfoResponse{IsSucc: false, IsPred: false, Position: int64(n.ringId)}

	// if the new node is the successor, must set the pointer correctly, return response indicating that this is node is a predecessor
	if n.successor == nil || n.isNewSuccessor(int(in.Position)) {
		n.UpdateSuccessor(context.Background(), &pb.UpdateNeighbourRequest{Position: in.Position, Address: in.Address})
		response.IsPred = true
	}

	// if the new node is the predecessor, must set the pointer correctly, return response indicating that this is node is a successor
	if n.predecessor == nil || n.isNewPredecessor(int(in.Position)) {
		n.UpdatePredecessor(context.Background(), &pb.UpdateNeighbourRequest{Position: in.Position, Address: in.Address})
		response.IsSucc = true
	}

	fmt.Print("\n")
	return response, nil
}

// Add a file to the network
func (n *node) AddFile(stream pb.ConsistentHash_AddFileServer) error {
	// Recieve the file name
	req, err := stream.Recv()
	if err != nil {
		fmt.Println("Could not recieve the file upload")
		return errors.New("failed to upload the file")
	}

	filename := req.GetFilename()

	// Determine if the file belongs here, if it does, store it here, otherwise pass it on to the successor
	if n.BelongsHere(filename) {
		fileData := bytes.Buffer{}
		fmt.Println("Receiving: " + filename)

		// Recieve the rest of the file
		for {
			req, err := stream.Recv()

			// If the end of the file was reached, we have read the whole file
			if err == io.EOF {
				break
			}

			if err != nil {
				return errors.New("error uploading file")
			}

			// Get chunk and write it to a buffer so we can save it to a file later
			chunk := req.GetChunkData()
			_, err = fileData.Write((chunk))
			if err != nil {
				return errors.New("error writing the file to buffer")
			}
		}

		err = stream.SendAndClose(&pb.UploadStatus{Message: "Closing the stream"})
		if err != nil {
			return errors.New("error closing the stream")
		}

		// Save the file with the recieved contents to this node
		_ = os.Mkdir("./"+n.storage, os.ModePerm)
		f, _ := os.Create("./" + n.storage + "/" + filename)
		defer f.Close()
		f.Write(fileData.Bytes())

		fmt.Println("Successfully uploaded " + filename + " and stored on node: " + n.Name)
	} else {
		// Setup a new stream with the successor of this node
		newStream, err := n.successor.client.AddFile(context.Background())
		if err != nil {
			fmt.Println("Could not open stream with sucessor!")
			return errors.New("error sending opening stream with sucessor")
		}

		// Send the filename to the successor
		req := &pb.UploadFileRequest{
			Data: &pb.UploadFileRequest_Filename{
				Filename: filename,
			},
		}
		err = newStream.Send(req)
		if err != nil {
			fmt.Println("Could not send filename to sucessor!")
			return errors.New("error sending filename to successor sucessor")
		}

		// Recieve the rest of the file
		for {
			// Recieve file from whoever is sending it
			req, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				fmt.Println("error uploading file")
				return errors.New("error uploading file")
			}

			// Get the chunk from the sender
			chunk := req.GetChunkData()

			// Pass the chunk on to the successor
			succReq := &pb.UploadFileRequest{
				Data: &pb.UploadFileRequest_ChunkData{
					ChunkData: chunk,
				},
			}
			err = newStream.Send(succReq)
			if err != nil {
				return errors.New("error writing the file to sucessor's buffer")
			}
		}

		err = stream.SendAndClose(&pb.UploadStatus{Message: "Closing the stream"})
		if err != nil {
			fmt.Println("error closing the stream with the sender")
			return errors.New("error closing the stream")
		}

		_, err = newStream.CloseAndRecv()
		if err != nil {
			fmt.Println("error closing the stream with the reciever")
		}

		fmt.Println("Upload of " + filename + " successfully forwarded to my successor!")
	}

	return nil
}

// Search for the specified file in the network, then return it to the original client
func (n *node) GetFile(ctx context.Context, in *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	// If the file should be here, send it back to the client that requested it
	if n.BelongsHere(in.Filename) {
		// Setup a connection to the client that requested the file
		conn, _ := grpc.Dial(in.ReturnAddress, grpc.WithInsecure())
		defer conn.Close()
		client := pb.NewClientReceiveServiceClient(conn)

		// Open the file
		file, err := os.Open("./" + n.storage + "/" + in.Filename)
		if err != nil {
			client.NotifyFileNotFound(context.Background(), &pb.FileNotFoundRequest{Message: "File not found!"})
			fmt.Println(in.Filename + "could not be found!")
			return &pb.GetFileResponse{Message: "Searching for file..."}, nil
		}
		defer file.Close()

		// Open the stream and send the filename across
		stream, err := client.ReceiveFile(context.Background())
		if err != nil {
			fmt.Println("Could not open stream with client!")
			fmt.Println(err)
			return &pb.GetFileResponse{Message: "Searching for file..."}, nil
		}
		req := &pb.AddFileRequest{
			Data: &pb.AddFileRequest_Filename{
				Filename: in.Filename,
			},
		}
		err = stream.Send(req)
		if err != nil {
			fmt.Println("Could not send filename")
			return &pb.GetFileResponse{Message: "Searching for file..."}, nil
		}

		// Read the file and send it across the stream in chunks
		reader := bufio.NewReader(file)
		buffer := make([]byte, 1024)
		for {
			n, err := reader.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("Could not read chunk from file")
				return &pb.GetFileResponse{Message: "Searching for file..."}, nil
			}

			req := &pb.AddFileRequest{
				Data: &pb.AddFileRequest_ChunkData{
					ChunkData: buffer[:n],
				},
			}

			err = stream.Send(req)
			if err != nil {
				fmt.Println("Could not send file to server")
			}
		}

		// close stream
		_, err = stream.CloseAndRecv()
		if err != nil {
			fmt.Println("Could not close the stream properly")
		}

		fmt.Println("File uploaded successfully!")
	} else {
		fmt.Println(in.Filename + " would not be stored on this node... passing request to successor")
		n.successor.client.GetFile(context.Background(), in)
	}

	return &pb.GetFileResponse{Message: "Searching for file..."}, nil
}

/* Helper functions */
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

// Helper function to determine if a new node is the new predecessor of this node
func (n *node) BelongsHere(filename string) bool {
	if n.predecessor == nil {
		return true
	}
	pos := n.HashString((filename))
	localPos := n.ringId
	predPos := n.predecessor.position

	// Common situation where the current node has a larger position than the predecessor node
	if localPos > predPos && pos <= localPos && pos > predPos {
		return true
	}

	// Other situation where predeccesor is at beginning of the ring, so its position is technically larger than this node
	if localPos < predPos && ((pos > predPos && pos < n.ringSize) || pos <= localPos && pos >= 0) {
		return true
	}

	return false
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
	// If its a file, have it named testfile_6 for example, and it will go at position 6
	inputList := strings.Fields(value)
	if len(inputList) > 1 {
		pos, _ := strconv.Atoi(inputList[1])
		return int(math.Mod(float64(pos), float64(10)))
	}
	pos := string(value[len(value)-1])
	temp, _ := strconv.ParseFloat(pos, 64)
	return int(math.Mod(temp, float64(10)))
}

// Main method of server
func main() {
	// pass the port as an argument and also the port of the other node
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Arguments required: <node name> <:port> <consul address>")
		return
	}
	node_name := args[0]
	port := args[1]
	sdaddress := args[2]

	// Stores all information about the server in a struct
	noden := node{Name: node_name, Addr: port, SDAddress: sdaddress, ringSize: 10, ringId: -1, storage: node_name}

	ctrlC := make(chan os.Signal)
	signal.Notify(ctrlC, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctrlC
		noden.Shutdown()
		os.Exit(1)
	}()

	// start the node
	noden.Start()
}

/* Startup functions */
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

// When a node shuts down, notify the network and send files to the successor to be stored
func (n *node) Shutdown() {

	fmt.Println("Node shutting down...")

	// Remove the node from the ring, update the successor's predecessor, and the predecessor's successor
	if n.successor != nil && n.predecessor != nil && n.successor.position != n.predecessor.position {
		response, _ := n.successor.client.GetNodeInfo(context.Background(), &pb.Empty{})
		succAddress := response.Address
		succPos := response.Position

		response, _ = n.predecessor.client.GetNodeInfo(context.Background(), &pb.Empty{})
		predAddress := response.Address
		predPos := response.Position

		n.successor.client.UpdatePredecessor(context.Background(), &pb.UpdateNeighbourRequest{Position: predPos, Address: predAddress})
		n.predecessor.client.UpdateSuccessor(context.Background(), &pb.UpdateNeighbourRequest{Position: succPos, Address: succAddress})
		n.SendAllFilesToSuccessor()
	} else if n.successor != nil && n.predecessor != nil {
		n.successor.client.UpdatePredecessor(context.Background(), &pb.UpdateNeighbourRequest{Position: -1})
		n.predecessor.client.UpdateSuccessor(context.Background(), &pb.UpdateNeighbourRequest{Position: -1})
		n.SendAllFilesToSuccessor()
	} else {
		fmt.Println("This is the last node on the ring, so no files will be accessible...")
	}
}

func (n *node) SendAllFilesToSuccessor() {
	items, err := ioutil.ReadDir("./" + n.storage)
	if err != nil {
		fmt.Println("There are no files stored on this node...")
		return
	}

	for _, item := range items {
		// Open the file
		file, err := os.Open("./" + n.storage + "/" + item.Name())
		if err != nil {
			fmt.Println("Something went wrong when opening the file!")
			return
		}
		defer file.Close()

		// Get the file name
		filename := item.Name()

		// Open the stream and send the filename across
		stream, err := n.successor.client.AddFile(context.Background())
		if err != nil {
			fmt.Println("Could not open stream with server!")
			return
		}
		req := &pb.UploadFileRequest{
			Data: &pb.UploadFileRequest_Filename{
				Filename: filename,
			},
		}
		err = stream.Send(req)
		if err != nil {
			fmt.Println("Could not send filename")
			return
		}

		// Read the file and send it across the stream in chunks
		reader := bufio.NewReader(file)
		buffer := make([]byte, 1024)
		for {
			n, err := reader.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("Could not read chunk from file")
				return
			}

			req := &pb.UploadFileRequest{
				Data: &pb.UploadFileRequest_ChunkData{
					ChunkData: buffer[:n],
				},
			}

			err = stream.Send(req)
			if err != nil {
				fmt.Println("Could not send file to server")
			}
		}

		// close stream
		_, err = stream.CloseAndRecv()
		if err != nil {
			fmt.Println("Could not close the stream properly")
		}

		fmt.Println("File uploaded successfully!")
	}
}
