package raft

type Command struct {
	Op    string
	Key   string
	Value string
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}
