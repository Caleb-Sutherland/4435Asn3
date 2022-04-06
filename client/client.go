package main

import (
	pb "4435Asn3/proto"
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)

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
	fmt.Println(response)
	fmt.Print("\nWelcome to the consistent hashing network...\nType 'search <filename>' to find a file\nType 'add <filename> to add a file\n\n")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		inputList := strings.Fields(input)

		if len(inputList) < 2 {
			fmt.Print("Invalid command, please use the following: \nType 'search <filename>' to find a file\nType 'add <filename> to add a file\n\n")
			continue
		}

		command := strings.ToLower(inputList[0])
		//filename := inputList[1]

		switch command {
		case "search":
			fmt.Print("Search command executing...\n\n")
		case "add":
			fmt.Print("Add command executing...\n\n")
		}
	}
}
