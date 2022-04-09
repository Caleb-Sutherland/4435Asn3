package main

import (
	pb "4435Asn3/proto"
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)

type clientserver struct {
	pb.UnimplementedClientReceiveServiceServer
}

var client_name = ""

func (c *clientserver) NotifyFileNotFound(ctx context.Context, in *pb.FileNotFoundRequest) (*pb.FileNotFoundResponse, error) {
	fmt.Println("The file you requested could not be found in the network!")
	return &pb.FileNotFoundResponse{Message: "Message received"}, nil
}

func (c *clientserver) ReceiveFile(stream pb.ClientReceiveService_ReceiveFileServer) error {
	req, err := stream.Recv()
	if err != nil {
		fmt.Println("Could not receive file from node")
		return errors.New("failed to upload file")
	}
	fileData := bytes.Buffer{}
	filename := req.GetFilename()
	fmt.Println("RECEIVING: " + filename)

	for {
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
	}

	err = stream.SendAndClose(&pb.AddStatus{Message: "Closing the stream"})
	if err != nil {
		return errors.New("error closing the stream")
	}

	// Save the file with the recieved contents to this node
	_ = os.Mkdir("./"+client_name, os.ModePerm)
	f, _ := os.Create("./" + client_name + "/" + filename)
	defer f.Close()
	f.Write(fileData.Bytes())

	fmt.Println(filename + " successfully downloaded and stored for client: " + client_name)
	return nil

}

func main() {

	// Determine the port being used for the server we are connecting to
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Arguments required: <port_of_node_to_connect_to> <port_for_this_client_to_be_reached> <client_name>")
		return
	}
	connectPort := args[0]
	localPort := args[1]
	name := args[2]
	client_name = name

	addr := flag.String("addr", "localhost:"+connectPort, "the address to connect to")

	// Connection to server with client
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewConsistentHashClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Test connnection with the node we are connecting to
	response, err := client.TestConnection(ctx, &pb.TestRequest{Message: "Hello"})
	if err != nil {
		fmt.Println("Failed to connect to server...")
		return
	}

	// Recieve commands
	go TakeCommands(client, localPort)

	// Indicate that the test message was recieved successfully
	fmt.Println(response.Message)
	fmt.Print("\nWelcome to the consistent hashing network...\nType 'query <filename>' to retrieve a file\nType 'add <filepath> to add a file into the network\n\n")

	// Set up this client as a server, so that it can also receive files from the network, upon request
	ServerListen(localPort)
}

// Function to be called as a go routine
func ServerListen(port string) {
	//Set up this client as a server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	_n := grpc.NewServer() // n is for serving purpose
	pb.RegisterClientReceiveServiceServer(_n, &clientserver{})
	//reflection.Register(_n)
	if err := _n.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func TakeCommands(client pb.ConsistentHashClient, port string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		inputList := strings.Fields(input)

		if len(inputList) < 2 {
			fmt.Print("Invalid command, please use the following: \nType 'query <filename>' to retrieve a file\nType 'add <filepath> to add a file into the network\n\n")
			continue
		}

		command := strings.ToLower(inputList[0])
		filepath := inputList[1]

		switch command {
		case "query":
			fmt.Print("Search command executing...\n\n")
			QueryFile(client, filepath, port)
		case "add":
			fmt.Print("Add command executing...\n\n")
			SendFile(client, filepath)
		}
	}
}

// Send a file into the network
func SendFile(client pb.ConsistentHashClient, filepath string) {

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Something went wrong when opening the file! Please try putting './' before the filename, or the path to the file!")
		return
	}
	defer file.Close()

	// Get the file name from the path
	pieces := strings.Split(filepath, "/")
	filename := pieces[len(pieces)-1]

	// Open the stream and send the filename across
	stream, err := client.AddFile(context.Background())
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

func QueryFile(client pb.ConsistentHashClient, filename string, returnAddress string) {
	client.GetFile(context.Background(), &pb.GetFileRequest{Filename: filename, ReturnAddress: ":" + returnAddress})
}
