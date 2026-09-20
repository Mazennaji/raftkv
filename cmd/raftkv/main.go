package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Mazennaji/raftkv/kv"
	"github.com/Mazennaji/raftkv/raft"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: raftkv <node-id> [wal-path]")
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

	walPath := fmt.Sprintf("node%d.wal", id)
	if len(os.Args) == 3 {
		walPath = os.Args[2]
	}
	wal, err := raft.NewWAL(walPath)
	if err != nil {
		panic(err)
	}

	n := raft.NewNode(id, cfg.Nodes, wal)
	store := kv.NewStore(n)
	n.OnApply(store.Apply)
	n.SetReader(store)

	if err := raft.Serve(n, self.Address); err != nil {
		panic(err)
	}
	n.Run()

	fmt.Printf("node %d listening on %s\n", id, self.Address)

	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Printf("node %d state: %s term: %d\n", id, n.State(), n.Term())
		}
	}()

	select {}
}
