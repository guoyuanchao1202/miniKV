package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/utils"
	"context"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// Raft A Go object implementing a single Raft peer.
type Raft struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         *sync.WaitGroup     // exit use
	status     int32               // set by Kill()
	mu         sync.RWMutex        // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	halfPeer   int                 // most peers num
	persister  *Persister          // Object to hold this peer's persisted state
	self       int                 // this peer's index into peers[]
	state      *State              // raft state
	applyCh    chan ApplyMsg       // apply message to up layer

}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.state.currentTerm, rf.state.role == LeaderRole
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state.role != LeaderRole {
		return InvalidIndex, rf.state.currentTerm, false
	}

	DPrintf("Leader(Term[%d]) %d receive command: %v", rf.state.currentTerm, rf.self, command)

	entry := rf.addToLogCache(command)

	return entry.Index, entry.Term, true
}

func (rf *Raft) addToLogCache(command interface{}) LogEntry {
	entry := LogEntry{
		Term:  rf.state.currentTerm,
		Index: rf.state.nextIndex[rf.self],
		Data:  command,
	}

	// leader will increase self nextIndex and matchIndex when receive new command from up layer
	rf.state.nextIndex[rf.self]++
	rf.state.matchIndex[rf.self]++
	rf.state.log = append(rf.state.log, entry)
	return entry
}

//
// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	if !atomic.CompareAndSwapInt32(&rf.status, Running, Killing) {
		return
	}
	rf.cancelFunc()
	rf.wg.Wait()

	// Your code here, if desired.

	atomic.StoreInt32(&rf.status, Killed)
}

// RequestVote  RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = args.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = rf.tryVote(args)
	if reply.VoteGranted {
		rf.state.votedFor = args.CandidateID
		rf.markAccessed()
	}

	if rf.state.currentTerm > args.Term {
		reply.Term = rf.state.currentTerm
	} else if rf.state.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	DPrintf("server %d vote to %d, reply.Term: %d, reply.Vote: %t", rf.self, args.CandidateID, reply.Term, reply.VoteGranted)

	return
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state.currentTerm = newTerm
	rf.state.role = FollowerRole
}

func (rf *Raft) isFollower() bool {
	return rf.state.role == FollowerRole
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = args.Term
	reply.Success = true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server(Term[%d], commitIndex[%d], nextIndex[%d], matchIndex[%d]) %d receive entries from Leader(Term[%d], LeaderCommit[%d], PrevLogIndex[%d], PrevLogTerm[%d], EntriesNum[%d]) %d",
		rf.state.currentTerm, rf.state.commitIndex, rf.state.nextIndex[rf.self], rf.state.matchIndex[rf.self],
		rf.self, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderID)

	// old leader's heartbeat
	if rf.state.currentTerm > args.Term {
		DPrintf("server %d term > old leader %d term, (%d > %d)", rf.self, args.LeaderID, rf.state.currentTerm, args.Term)
		reply.Term = rf.state.currentTerm
		reply.Success = false
		return
	}

	reply.NextIndex = rf.state.nextIndex[rf.self]
	reply.MatchIndex = rf.state.matchIndex[rf.self]
	rf.markAccessed()
	rf.becomeFollower(args.Term)

	// current follower log is much behind.
	if args.PrevLogIndex > reply.MatchIndex {
		DPrintf("Server %d matchIndex[%d] < Leader %d prevLogIndex[%d], return",
			rf.self, rf.state.matchIndex[rf.self], args.LeaderID, args.PrevLogIndex)
		return
	}

	// conflict
	if rf.state.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Server(PrevLogTerm[%d]) %d conflict with Leader(PrevLogTerm[%d]) %d, conflictIndex: %d",
			rf.state.log[args.PrevLogIndex].Term, rf.self, args.PrevLogTerm, args.LeaderID, args.PrevLogIndex)
		rf.removeConflictLog(args.PrevLogIndex)
		reply.NextIndex = args.PrevLogIndex
		reply.MatchIndex = args.PrevLogIndex - 1
		reply.Success = false
		return
	}

	// after checking illegal, then do normal thing

	// no new log entries
	if len(args.Entries) == 0 {
		DPrintf("No entries, server %d oldCommitIndex: %d, leaderCommit: %d, matchIndex: %d",
			rf.self, rf.state.commitIndex, args.LeaderCommit, rf.state.matchIndex[rf.self])
		rf.state.commitIndex = utils.Min(args.LeaderCommit, rf.state.matchIndex[rf.self])
		return
	}

	// oldState := rf.state.clone()

	newMatchIndex := args.PrevLogIndex + len(args.Entries)
	newNextIndex := newMatchIndex + 1
	start := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		if start+i >= len(rf.state.log) {
			rf.state.log = append(rf.state.log, args.Entries[i:]...)
			break
		}
		rf.state.log[start+i] = args.Entries[i]
	}

	if newMatchIndex >= len(rf.state.log) {
		DPrintf("========Old ServerInfo========")
		DPrintf("ServerID: %d, ServerState: %v", rf.self, rf.state)
		DPrintf("========New ServerInfo========")
		// DPrintf("ServerID: %d, ServerState: %v", rf.self, oldState)
		DPrintf("========AppendEntriesReq========")
		DPrintf("AppendEntriesArgs: %v", args)

		panic("!!!!!dangerous")
	}

	rf.state.commitIndex = utils.Min(args.LeaderCommit, newMatchIndex)

	rf.state.matchIndex[rf.self] = newMatchIndex
	rf.state.nextIndex[rf.self] = newNextIndex

	reply.NextIndex = newNextIndex
	reply.MatchIndex = newMatchIndex

	return
}

func (rf *Raft) removeConflictLog(newNextIndex int) {
	rf.state.log = rf.state.log[:newNextIndex]
	rf.state.matchIndex[rf.self] = newNextIndex - 1
	rf.state.nextIndex[rf.self] = newNextIndex
}

func (rf *Raft) markAccessed() {
	rf.state.accessed = true
}

func (rf *Raft) clearAccessed() {
	rf.state.accessed = false
}

func (rf *Raft) accessed() bool {
	return rf.state.accessed
}

// 1. if currentTerm < peer.Term || currentTerm hasn't voted to any or already voted to peer
// 2. and peer's log at least up to date as current
func (rf *Raft) tryVote(args *RequestVoteArgs) bool {
	lastLogIndex := len(rf.state.log) - 1
	lastLogTerm := rf.state.log[lastLogIndex].Term

	// candidate from old term or candidate's log is too old, refused
	if rf.state.currentTerm > args.Term ||
		args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm &&
			args.LastLogIndex < lastLogIndex) {
		return false
	}

	// same term but already voted to others, refused
	if rf.state.currentTerm == args.Term &&
		rf.state.votedFor != InitialVoteFor &&
		rf.state.votedFor != args.CandidateID {
		return false
	}

	// ok!!!
	return true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a status server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.status) == Killed
}

func (rf *Raft) election() {
	ticker := time.NewTicker(genRandomElectionTimeout(DefaultElectionMinTimeout, DefaultElectionMaxTimeout))
	for {
		ticker.Reset(genRandomElectionTimeout(DefaultElectionMinTimeout, DefaultElectionMaxTimeout))
		select {
		case <-rf.ctx.Done():
			rf.wg.Done()
			return
		case <-ticker.C:
			requestVoteArgs, needElection := rf.prepareElection()
			if !needElection {
				continue
			}

			DPrintf("server: %d start election, currentTerm: %d", rf.self, requestVoteArgs.Term)

			rf.doElection(requestVoteArgs)
		}
	}
}

func (rf *Raft) prepareElection() (*RequestVoteArgs, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.accessed() || rf.state.role == LeaderRole {
		rf.clearAccessed()
		return nil, false
	}

	rf.becomeCandidate()

	return rf.genRequestVoteArgs(), true

}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex := len(rf.state.log) - 1
	lastLogTerm := rf.state.log[lastLogIndex].Term

	return &RequestVoteArgs{
		Term:         rf.state.currentTerm,
		CandidateID:  rf.self,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (rf *Raft) becomeCandidate() {
	rf.state.role = CandidateRole
	rf.state.currentTerm++
	rf.state.votedFor = rf.self
}

func (rf *Raft) doElection(requestVoteArgs *RequestVoteArgs) {
	repliesCh := make(chan *RequestVoteReply, len(rf.peers))

	rf.sendVoteRequest(requestVoteArgs, repliesCh)
	maxTerm, voteGrantedNum := rf.tryCollectVoteReply(repliesCh)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isFollower() || maxTerm > rf.state.currentTerm || voteGrantedNum <= rf.halfPeer {
		rf.becomeFollower(utils.Max(maxTerm, rf.state.currentTerm))
		return
	}

	rf.becomeLeader()
	// rf.state.votedFor = -1
}

// becomeLeader need lock outside
func (rf *Raft) becomeLeader() {
	rf.state.role = LeaderRole
	nextIndex := len(rf.state.log)
	for i := range rf.state.nextIndex {
		rf.state.nextIndex[i] = nextIndex
	}

	DPrintf("server %d become new Leader, reset nextIndex to %d, currentLog: %v", rf.self, nextIndex, rf.state.log)
}

func (rf *Raft) sendVoteRequest(requestVoteArgs *RequestVoteArgs, repliesCh chan *RequestVoteReply) {
	for i := range rf.peers {
		if i == rf.self {
			continue
		}
		go func(serverIndex int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(serverIndex, requestVoteArgs, reply) {
				return
			}
			repliesCh <- reply
		}(i)
	}
}

func (rf *Raft) tryCollectVoteReply(repliesCh chan *RequestVoteReply) (int, int) {
	voteGrantedNum := 1
	refusedReplies := make([]*RequestVoteReply, 0)
	timeout := time.NewTicker(DefaultTimeoutMilliseconds * time.Millisecond)

	for i := 0; i < len(rf.peers)-1 && voteGrantedNum <= rf.halfPeer; i++ {
		select {
		case reply := <-repliesCh:
			if reply.VoteGranted {
				voteGrantedNum++
				continue
			}

			if reply.Term == -1 {
				continue
			}

			refusedReplies = append(refusedReplies, reply)
		case <-timeout.C:
			break
		}
	}

	maxTerm := 0
	for _, reply := range refusedReplies {
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
	}

	return maxTerm, voteGrantedNum
}

func (rf *Raft) heartbeat() {
	heartbeatTicker := time.NewTicker(DefaultHeartbeatIntervalMilliseconds * time.Millisecond)
	for {
		heartbeatTicker.Reset(DefaultHeartbeatIntervalMilliseconds * time.Millisecond)
		select {
		case <-rf.ctx.Done():
			rf.wg.Done()
			return
		case <-heartbeatTicker.C:
			if _, isLeader := rf.GetState(); !isLeader {
				continue
			}

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.self {
					continue
				}
				go rf.doSendAppendEntriesRequest(i)
			}
		}
	}
}

func (rf *Raft) checkAndIncCommitIndex() {
	ticker := time.NewTicker(DefaultCheckCommitIndexInterval * time.Millisecond)
	matchIndex := make([]int, len(rf.peers))
	oldCommitIndex := 0
	for {
		ticker.Reset(10 * time.Millisecond)
		select {
		case <-rf.ctx.Done():
			rf.wg.Done()
			return
		case <-ticker.C:
			if _, leader := rf.GetState(); !leader {
				continue
			}

			rf.mu.RLock()
			oldCommitIndex = rf.state.commitIndex
			copy(matchIndex, rf.state.matchIndex)
			rf.mu.RUnlock()

			newIndex, increased := rf.tryIncCommitIndex(oldCommitIndex, matchIndex)
			if !increased {
				continue
			}

			rf.mu.Lock()
			if rf.state.log[newIndex].Term == rf.state.currentTerm {
				rf.state.commitIndex = newIndex
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) tryIncCommitIndex(oldCommitIndex int, matchIndex []int) (int, bool) {
	newIndex := -1
	for i := oldCommitIndex + 1; ; i++ {
		peerNum := 0
		for _, match := range matchIndex {
			if match >= i {
				peerNum++
			}
		}
		if peerNum > rf.halfPeer {
			newIndex = i
		} else {
			break
		}
	}

	if newIndex == -1 {
		return -1, false
	}

	return newIndex, true
}

func (rf *Raft) applyMessage() {
	ticker := time.NewTicker(DefaultApplyMsgIntervalMilliseconds * time.Millisecond)
	for {
		ticker.Reset(DefaultApplyMsgIntervalMilliseconds * time.Millisecond)
		select {
		case <-rf.ctx.Done():
			rf.wg.Done()
			return
		case <-ticker.C:
			rf.mu.RLock()
			// DPrintf("server %d try to apply message, lastApplied: %d, commitIndex: %d", rf.self, rf.state.lastApplied, rf.state.commitIndex)
			toApplyEntries := make([]ApplyMsg, 0, rf.state.commitIndex-rf.state.lastApplied)
			// toApplyEntries := make([]ApplyMsg, 0)
			for i := rf.state.lastApplied + 1; i <= rf.state.commitIndex; i++ {
				toApplyEntries = append(toApplyEntries, ApplyMsg{
					CommandValid: true,
					Command:      rf.state.log[i].Data,
					CommandIndex: i,
				})
			}
			rf.state.lastApplied = rf.state.commitIndex
			rf.mu.RUnlock()

			if len(toApplyEntries) > 0 {
				DPrintf("server %d commitIndex: %d, lastApplied: %d, applyMsg: %d", rf.self, rf.state.commitIndex, rf.state.lastApplied, len(toApplyEntries))
			}
			for _, applyMsg := range toApplyEntries {
				rf.applyCh <- applyMsg
			}
		}
	}
}

//
// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[self]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, self int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := newRaft(peers, self, persister, applyCh) // get raft instance

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.election()               // start election goroutine to start elections
	go rf.heartbeat()              // leader use, send heartbeat and log to follower
	go rf.checkAndIncCommitIndex() // leader use, check and inc commitIndex
	go rf.applyMessage()           // all servers use, apply message to up layer

	rf.wg.Add(4)

	return rf
}

func (rf *Raft) doSendAppendEntriesRequest(dstServerIndex int) {
	var (
		prevLogIndex int
		prevLogTerm  int
		logNum       int
		currentTerm  int
		commitIndex  int
		toSendLog    []LogEntry
	)

	rf.mu.RLock()
	sendStartIndex := rf.state.nextIndex[dstServerIndex]
	prevLogIndex = sendStartIndex - 1
	prevLogTerm = rf.state.log[prevLogIndex].Term
	logNum = len(rf.state.log)
	currentTerm = rf.state.currentTerm
	toSendLog = make([]LogEntry, logNum-sendStartIndex)
	copy(toSendLog, rf.state.log[sendStartIndex:])
	commitIndex = rf.state.commitIndex

	DPrintf("Leader(Term[%d], commitIndex[%d], prevLogIndex[%d], prevLogTerm[%d]) %d send logEntries to follower %d, entries num: %d",
		currentTerm, commitIndex, prevLogIndex, prevLogTerm, rf.self, dstServerIndex, len(toSendLog))

	rf.mu.RUnlock()

	appendEntriesArgs := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     rf.self,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      toSendLog,
		LeaderCommit: commitIndex,
	}

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(dstServerIndex, appendEntriesArgs, reply) {
		return
	}

	DPrintf("Leader(Term[%d]) %d receive logEntries response from follower %d, entries num: %d", rf.state.currentTerm, rf.self, dstServerIndex, len(toSendLog))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// do nothing
	if rf.state.role != LeaderRole {
		return
	}

	// update follower's nextIndex and matchIndex
	rf.state.nextIndex[dstServerIndex] = utils.Min(len(rf.state.log), reply.NextIndex)
	rf.state.matchIndex[dstServerIndex] = rf.state.nextIndex[dstServerIndex] - 1

	if reply.Success {
		return
	}

	if rf.state.currentTerm < reply.Term {
		DPrintf("old leader(Term[%d]) %d receive higher term[%d] from server %d, become follower", rf.state.currentTerm, rf.self, reply.Term, dstServerIndex)
		rf.becomeFollower(reply.Term)
	}
}

func newRaft(peers []*labrpc.ClientEnd, self int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{
		wg:        &sync.WaitGroup{},
		mu:        sync.RWMutex{},
		peers:     peers,
		halfPeer:  len(peers) / 2,
		persister: persister,
		self:      self,
		state: &State{
			role:        InitialRole,
			currentTerm: InitialTerm,
			votedFor:    InitialVoteFor,
			commitIndex: InitialCommitIndex,
			lastApplied: InitialLastApplied,
			nextIndex:   genInitialNextIndex(len(peers)),
			matchIndex:  make([]int, len(peers)),
			log:         genInitialLog(),
		},
		applyCh: applyCh,
	}

	raft.ctx, raft.cancelFunc = context.WithCancel(context.Background())
	return raft
}
