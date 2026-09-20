package main

import (
	"fmt"

	"github.com/Mazennaji/raftkv/client"
)

func main() {
	c := client.New([]string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	})

	if err := c.Put("hello", "world"); err != nil {
		fmt.Println("put error:", err)
		return
	}
	fmt.Println("put succeeded")

	val, found, err := c.Get("hello")
	if err != nil {
		fmt.Println("get error:", err)
		return
	}
	fmt.Println("hello =", val, "found:", found)
}
