package main

import (
	"fmt"

	"github.com/Mazennaji/raftkv/kv"
)

func main() {
	store, err := kv.Open("raftkv.wal")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Put("foo", "bar")
	val, ok := store.Get("foo")
	fmt.Println("foo =", val, "found:", ok)
}
