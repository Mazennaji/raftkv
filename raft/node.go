package raft

import (
	"errors"
	"sync"
	"time"
)

type State int

var (
	ErrNotLeader = errors.New("not the leader")
	ErrNoQuorum  = errors.New("failed to reach quorum")
)

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

	lastHeartbeat time.Time

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

func (n *Node) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = 0
		n.state = Follower
	}

	lastLogIndex, lastLogTerm := n.lastLogInfo()
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (n.votedFor == 0 || n.votedFor == args.CandidateID) && logIsUpToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = n.currentTerm
	return nil
}

func (n *Node) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	n.currentTerm = args.Term
	n.state = Follower
	n.votedFor = 0
	n.lastHeartbeat = time.Now()

	if len(args.Entries) > 0 {
		n.log = args.Entries
		if n.wal != nil {
			for _, entry := range args.Entries {
				n.wal.Append(entry)
			}
		}
	}

	reply.Term = n.currentTerm
	reply.Success = true
	return nil
}

func (n *Node) lastLogInfo() (index uint64, term uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) Propose(command Command) (uint64, error) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	index := uint64(len(n.log)) + 1
	entry := LogEntry{
		Index:   index,
		Term:    n.currentTerm,
		Command: command,
	}

	if n.wal != nil {
		if err := n.wal.Append(entry); err != nil {
			n.mu.Unlock()
			return 0, err
		}
	}
	n.log = append(n.log, entry)
	term := n.currentTerm
	peers := n.peers
	n.mu.Unlock()

	acked := 1
	ackCh := make(chan bool, len(peers))

	for _, peer := range peers {
		if peer.ID == n.id {
			continue
		}
		go func(p NodeConfig) {
			ok := n.replicateTo(p, term)
			ackCh <- ok
		}(peer)
	}

	needed := len(peers)/2 + 1
	for i := 0; i < len(peers)-1; i++ {
		if <-ackCh {
			acked++
		}
		if acked >= needed {
			return index, nil
		}
	}

	return index, ErrNoQuorum
}

func (n *Node) replicateTo(peer NodeConfig, term uint64) bool {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}
	entries := make([]LogEntry, len(n.log))
	copy(entries, n.log)
	prevLogIndex, prevLogTerm := uint64(0), uint64(0)
	if len(entries) > 1 {
		prev := entries[len(entries)-2]
		prevLogIndex, prevLogTerm = prev.Index, prev.Term
	}
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}

	reply, err := callAppendEntries(peer.Address, args)
	if err != nil {
		return false
	}

	n.mu.Lock()
	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.state = Follower
		n.votedFor = 0
	}
	n.mu.Unlock()

	return reply.Success
}
