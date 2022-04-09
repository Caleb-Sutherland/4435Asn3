package main

import (
	pb "4435Asn3/proto"
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)
type clientserver struct{
	pb.UnimplementedClientReceiveServiceServer
}

func (c *clientserver) ReceiveFile(stream pb.ClientReceiveService_ReceiveFileServer) error {
	req, err := stream.Recv()
	if err != nil{
		fmt.Println("Could not receive file from node")
		return erros.New("failed to upload file")
	}
	fileData := bytes.Buffer{}
	filename := req.GetFilename()
	fmt.Println("RECEIVING: " + filename)

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


func main() {

	// Determine the port being used for the server we are connecting to
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Println("Arguments required: <port_of_node_to_connect_to>")
		return
	}
	port := args[0]

	addr := flag.String("addr", "localhost:"+port, "the address to connect to")

	// Connection to server with client
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewConsistentHashClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, _ := client.TestConnection(ctx, &pb.TestRequest{Message: "Hello"})

	//Set up client server
	c := grpc.NewServer()
	pb.RegisterClientReceiveServiceServer(c, &clientserver{})

	fmt.Println(response)
	fmt.Print("\nWelcome to the consistent hashing network...\nType 'search <filename>' to find a file\nType 'add <filename> to add a file\n\n")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		inputList := strings.Fields(input)

		if len(inputList) < 2 {
			fmt.Print("Invalid command, please use the following: \nType 'search <filepath>' to find a file\nType 'add <filename> to add a file\n\n")
			continue
		}

		command := strings.ToLower(inputList[0])
		filepath := inputList[1]

		switch command {
		case "search":
			fmt.Print("Search command executing...\n\n")
		case "add":
			fmt.Print("Add command executing...\n\n")
			SendFile(client, filepath)
		}
	}
}

func SendFile(client pb.ConsistentHashClient, filepath string) {

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Something went wrong when opening the file!")
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

