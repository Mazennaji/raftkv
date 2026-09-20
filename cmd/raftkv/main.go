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

	wal, err := raft.NewWAL(fmt.Sprintf("node%d.wal", id))
	if err != nil {
		panic(err)
	}

	n := raft.NewNode(id, cfg.Nodes, wal)
	store := kv.NewStore(n)
	n.OnApply(store.Apply)

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

	time.Sleep(2 * time.Second)
	if n.State() == raft.Leader {
		if err := store.Put("foo", "bar"); err != nil {
			fmt.Println("put failed:", err)
		} else {
			fmt.Println("put foo=bar succeeded")
		}
	}

	select {}
}
