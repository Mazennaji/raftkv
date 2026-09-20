package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Mazennaji/raftkv/raft"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: raftkv <node-id>")
		os.Exit(1)
	}

	id, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		panic(err)
	}

	cfg, err := raft.LoadConfig("config/cluster.yaml")
	if err != nil {
		panic(err)
	}

	var self raft.NodeConfig
	for _, node := range cfg.Nodes {
		if node.ID == id {
			self = node
		}
	}

	n := raft.NewNode(id, cfg.Nodes, nil)
	if err := raft.Serve(n, self.Address); err != nil {
		panic(err)
	}
	n.Run()

	fmt.Printf("node %d listening on %s\n", id, self.Address)

	for {
		time.Sleep(1 * time.Second)
		fmt.Printf("node %d state: %s term: %d\n", id, n.State(), n.Term())
	}
}
