package raft

import (
	"math/rand"
	"time"
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (n *Node) Run() {
	go n.electionLoop()
}

func (n *Node) electionLoop() {
	for {
		timeout := randomElectionTimeout()
		time.Sleep(timeout)

		n.mu.Lock()
		state := n.state
		sinceHeartbeat := time.Since(n.lastHeartbeat)
		n.mu.Unlock()

		if state != Leader && sinceHeartbeat >= timeout {
			n.startElection()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	peers := n.peers
	n.mu.Unlock()

	votes := 1 // vote for self
	votesNeeded := len(peers)/2 + 1

	voteCh := make(chan bool, len(peers))

	for _, peer := range peers {
		if peer.ID == n.id {
			continue
		}
		go func(address string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.doRequestVote(address, args)
			if err != nil {
				voteCh <- false
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = 0
			}
			n.mu.Unlock()

			voteCh <- reply.VoteGranted
		}(peer.Address)
	}

	for i := 0; i < len(peers)-1; i++ {
		if <-voteCh {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Candidate && n.currentTerm == term && votes >= votesNeeded {
		n.state = Leader
		n.resetNextIndex()
		go n.leaderLoop()
	}
}

func (n *Node) leaderLoop() {
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		peers := n.peers
		n.mu.Unlock()

		for _, peer := range peers {
			if peer.ID == n.id {
				continue
			}
			go func(address string) {
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderID: n.id,
				}
				reply, err := n.doAppendEntries(address, args)
				if err != nil {
					return
				}
				n.mu.Lock()
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.state = Follower
					n.votedFor = 0
				}
				n.mu.Unlock()
			}(peer.Address)
		}

		time.Sleep(heartbeatInterval)
	}
}
