package main

import (
	"fmt"
	"os"

	"github.com/Mazennaji/raftkv/kv"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: raftkv put <key> <value> | raftkv get <key>")
		os.Exit(1)
	}

	store, err := kv.Open("raftkv.wal")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	switch os.Args[1] {
	case "put":
		if len(os.Args) != 4 {
			fmt.Println("usage: raftkv put <key> <value>")
			os.Exit(1)
		}
		key, value := os.Args[2], os.Args[3]
		if err := store.Put(key, value); err != nil {
			panic(err)
		}
		fmt.Printf("put %s = %s\n", key, value)

	case "get":
		if len(os.Args) != 3 {
			fmt.Println("usage: raftkv get <key>")
			os.Exit(1)
		}
		key := os.Args[2]
		val, ok := store.Get(key)
		if !ok {
			fmt.Printf("%s not found\n", key)
			os.Exit(1)
		}
		fmt.Printf("%s = %s\n", key, val)

	default:
		fmt.Println("unknown command:", os.Args[1])
		os.Exit(1)
	}
}
