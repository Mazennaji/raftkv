package raft

import (
	"net"
	"net/rpc"
)

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
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

type RPCHandler struct {
	node *Node
}

func (h *RPCHandler) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return h.node.HandleRequestVote(args, reply)
}

func (h *RPCHandler) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return h.node.HandleAppendEntries(args, reply)
}

func (h *RPCHandler) ClientPut(args *ClientPutArgs, reply *ClientPutReply) error {
	if h.node.State() != Leader {
		reply.NotLeader = true
		return nil
	}
	_, err := h.node.Propose(Command{Op: "PUT", Key: args.Key, Value: args.Value})
	if err == ErrNotLeader {
		reply.NotLeader = true
		return nil
	}
	return err
}

func (h *RPCHandler) ClientGet(args *ClientGetArgs, reply *ClientGetReply) error {
	if h.node.State() != Leader {
		reply.NotLeader = true
		return nil
	}
	value, found := h.node.reader.Get(args.Key)
	reply.Value = value
	reply.Found = found
	return nil
}

func Serve(n *Node, address string) error {
	handler := &RPCHandler{node: n}
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", handler); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	go server.Accept(listener)
	return nil
}

func callRequestVote(address string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	reply := &RequestVoteReply{}
	if err := client.Call("Raft.RequestVote", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func callAppendEntries(address string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	reply := &AppendEntriesReply{}
	if err := client.Call("Raft.AppendEntries", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}