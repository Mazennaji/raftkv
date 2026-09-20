package raft

import "sync"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Node struct {
	mu sync.Mutex

	id    uint64
	peers []NodeConfig
	state State

	currentTerm uint64
	votedFor    uint64

	wal *WAL
	log []LogEntry
}

func NewNode(id uint64, peers []NodeConfig, wal *WAL) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    0,
	}
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}
