package raft

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNotLeader = errors.New("not the leader")
	ErrNoQuorum  = errors.New("failed to reach quorum")
)

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

type Reader interface {
	Get(key string) (string, bool)
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

	nextIndex map[uint64]uint64

	onApply func(LogEntry)
	reader  Reader
}

func NewNode(id uint64, peers []NodeConfig, wal *WAL) *Node {
	nextIndex := make(map[uint64]uint64)
	for _, p := range peers {
		nextIndex[p.ID] = 1
	}
	return &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    0,
		wal:         wal,
		nextIndex:   nextIndex,
	}
}

func (n *Node) OnApply(fn func(LogEntry)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onApply = fn
}

func (n *Node) SetReader(r Reader) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reader = r
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

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > uint64(len(n.log)) {
			reply.Term = n.currentTerm
			reply.Success = false
			return nil
		}
		existing := n.log[args.PrevLogIndex-1]
		if existing.Term != args.PrevLogTerm {
			n.log = n.log[:args.PrevLogIndex-1]
			reply.Term = n.currentTerm
			reply.Success = false
			return nil
		}
	}

	for _, entry := range args.Entries {
		if entry.Index <= uint64(len(n.log)) {
			existing := n.log[entry.Index-1]
			if existing.Term != entry.Term {
				n.log = n.log[:entry.Index-1]
				n.log = append(n.log, entry)
				if n.onApply != nil {
					n.onApply(entry)
				}
			}
			continue
		}
		n.log = append(n.log, entry)
		if n.wal != nil {
			n.wal.Append(entry)
		}
		if n.onApply != nil {
			n.onApply(entry)
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
			n.mu.Lock()
			if n.onApply != nil {
				n.onApply(entry)
			}
			n.mu.Unlock()
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
	nextIdx := n.nextIndex[peer.ID]
	if nextIdx == 0 {
		nextIdx = uint64(len(n.log)) + 1
	}
	n.mu.Unlock()

	for {
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return false
		}

		entries := make([]LogEntry, 0)
		if nextIdx <= uint64(len(n.log)) {
			entries = append(entries, n.log[nextIdx-1:]...)
		}

		prevLogIndex, prevLogTerm := uint64(0), uint64(0)
		if nextIdx > 1 {
			prev := n.log[nextIdx-2]
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
			n.mu.Unlock()
			return false
		}
		n.mu.Unlock()

		if reply.Success {
			n.setNextIndex(peer.ID, nextIdx+uint64(len(entries)))
			return true
		}

		if nextIdx > 1 {
			nextIdx--
		} else {
			return false
		}
	}
}

func (n *Node) nextIndexFor(peerID uint64) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	idx, ok := n.nextIndex[peerID]
	if !ok {
		return uint64(len(n.log)) + 1
	}
	return idx
}

func (n *Node) setNextIndex(peerID uint64, idx uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextIndex[peerID] = idx
}

func (n *Node) resetNextIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()
	next := uint64(len(n.log)) + 1
	for _, p := range n.peers {
		n.nextIndex[p.ID] = next
	}
}
