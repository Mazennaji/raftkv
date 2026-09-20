package client

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
)

var ErrAllNodesUnreachable = errors.New("no reachable node found a leader")

type Client struct {
	addresses []string
	leader    string
}

func New(addresses []string) *Client {
	return &Client{
		addresses: addresses,
	}
}

func (c *Client) Put(key, value string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := c.tryPut(key, value); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
			continue
		}
		return nil
	}
	return lastErr
}

func (c *Client) tryPut(key, value string) error {
	args := &ClientPutArgs{Key: key, Value: value}

	order := c.candidateOrder()
	for _, addr := range order {
		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("[client] dial %s failed: %v\n", addr, err)
			continue
		}
		reply := &ClientPutReply{}
		err = conn.Call("Raft.ClientPut", args, reply)
		conn.Close()
		if err != nil {
			fmt.Printf("[client] call to %s failed: %v\n", addr, err)
			continue
		}
		if reply.NotLeader {
			fmt.Printf("[client] %s says it is not leader\n", addr)
			continue
		}
		c.leader = addr
		return nil
	}

	return ErrAllNodesUnreachable
}

func (c *Client) Get(key string) (string, bool, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		val, found, err := c.tryGet(key)
		if err == nil {
			return val, found, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return "", false, lastErr
}

func (c *Client) tryGet(key string) (string, bool, error) {
	args := &ClientGetArgs{Key: key}

	order := c.candidateOrder()
	for _, addr := range order {
		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			continue
		}
		reply := &ClientGetReply{}
		err = conn.Call("Raft.ClientGet", args, reply)
		conn.Close()
		if err != nil {
			continue
		}
		if reply.NotLeader {
			continue
		}
		c.leader = addr
		return reply.Value, reply.Found, nil
	}

	return "", false, ErrAllNodesUnreachable
}

func (c *Client) candidateOrder() []string {
	if c.leader == "" {
		return c.addresses
	}
	order := []string{c.leader}
	for _, addr := range c.addresses {
		if addr != c.leader {
			order = append(order, addr)
		}
	}
	return order
}

type ClientPutArgs struct {
	Key   string
	Value string
}

type ClientPutReply struct {
	NotLeader bool
}

type ClientGetArgs struct {
	Key string
}

type ClientGetReply struct {
	Value     string
	Found     bool
	NotLeader bool
}
