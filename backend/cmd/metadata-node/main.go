package main

import (
	"cloud-storage/internal/metadata/raft"
	"cloud-storage/internal/metadata/raft/persistence"
	"flag"
	"fmt"
	"strings"
)

func main() {
	fmt.Println("Starting metadata node ...")
	id := flag.String("id", "node1", "node ID")
	port := flag.String("port", "5001", "port")
	peers := flag.String("peers", "", "comma separated peers")
	stateFile := flag.String("stateFile", *id + "_state.json", "state file name")
	logFile := flag.String("logFile", *id + "_log.json", "log file name")

	flag.Parse()

	address := "localhost:" + *port

	peerList := []string{}
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	storage := persistence.NewDistStorage(*logFile, *stateFile)

	node := raft.NewRaftNode(*id, address, peerList, storage);

	fmt.Printf("Starting Node %s at %s\n", *id, address);
	fmt.Println(node.Debug())
	
	// TODO: start RPC server
    // TODO: start election loop
	go node.StartHttpServer()
	go node.Run()
    select {} // keep running
}