package main

import (
	"cloud-storage/internal/metadata/raft"
	"fmt"
)

func main() {
	fmt.Println("Starting metadata node ...")
	
	node  := raft.NewRaftNode("node1", []string{"node2", "node3"})

	fmt.Println("Node created", node)
	
}