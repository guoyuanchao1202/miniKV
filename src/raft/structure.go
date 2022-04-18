package raft

//
// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RoleType int

const (
	UnKnowRole RoleType = iota
	FollowerRole
	CandidateRole
	LeaderRole
)


const (
	Running int32 = iota
	Killing
	Killed
)

type State struct {
	role        RoleType   // leader / follower / candidate
	currentTerm int        // current term
	votedFor    int        // vote peer id
	commitIndex int        // committed index
	lastApplied int        // applied index ( <= commitIndex)
	nextIndex   []int      // only leader use
	matchIndex  []int      // only leader use, used to inc commitIndex
	accessed    bool       // only follower use
	log         []LogEntry // logs
}

func (s *State) clone() *State {
	cState := &State{
		role:        s.role,
		currentTerm: s.currentTerm,
		votedFor:    s.votedFor,
		commitIndex: s.commitIndex,
		lastApplied: s.lastApplied,
		nextIndex:   make([]int, len(s.nextIndex)),
		matchIndex:  make([]int, len(s.matchIndex)),
		accessed:    s.accessed,
		log:         make([]LogEntry, len(s.log)),
	}

	copy(cState.nextIndex, s.nextIndex)
	copy(cState.matchIndex, s.matchIndex)
	copy(cState.log, s.log)
	return cState
}

type LogEntry struct {
	Term  int
	Index int
	Data  interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	NextIndex  int
	MatchIndex int
	Success    bool
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
