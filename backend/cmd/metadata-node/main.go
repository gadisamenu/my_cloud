package main

import (
	"cloud-storage/internal/metadata/raft"
	"flag"
	"fmt"
	"strings"
)

func main() {
	fmt.Println("Starting metadata node ...")
	id := flag.String("id", "node1", "node ID")
	port := flag.String("port", "5001", "port")
	peers := flag.String("peers", "", "comma separated peers")

	flag.Parse()

	address := "localhost:" + *port


	peerList := []string{}
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	node := raft.NewRaftNode(*id, address, peerList);

	fmt.Printf("Starting Node %s at %s\n", *id, address);
	fmt.Println(node.Debug())
	
	// TODO: start RPC server
    // TODO: start election loop
	go node.StartHttpServer()
	go node.Run()
    select {} // keep running
}