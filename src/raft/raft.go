package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/utils"
	"bytes"
	"context"
	"log"

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

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.state.CurrentTerm, rf.state.role == LeaderRole
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if err := encoder.Encode(rf.state.VotedFor); err != nil {
		DPrintf("Persist VoteFor failed, err: %s", err.Error())
	}
	if err := encoder.Encode(rf.state.CurrentTerm); err != nil {
		DPrintf("Persist CurrentTerm failed, err: %s", err.Error())
	}
	if err := encoder.Encode(rf.state.Log); err != nil {
		DPrintf("Persist Log failed, err: %s", err.Error())
	}

	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("Start without state")
		return nil
	}
	// Your code here (2C).
	// Example:
	rdBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(rdBuffer)
	if err := decoder.Decode(&rf.state.VotedFor); err != nil {
		DPrintf("Read persist VoteFor failed, err: %s", err.Error())
	}

	if err := decoder.Decode(&rf.state.CurrentTerm); err != nil {
		DPrintf("Read persist CurrentTerm failed, err: %s", err.Error())
	}

	if err := decoder.Decode(&rf.state.Log); err != nil {
		DPrintf("Read persist Log failed, err: %s", err.Error())
	}

	rf.state.nextIndex[rf.self] = len(rf.state.Log)
	if rf.state.nextIndex[rf.self] == 0 {
		log.Printf("Server %d readPersist, initNextIndex: %d", rf.self, len(rf.state.Log))
		panic("dangerous!!!!!")
	}

	rf.state.matchIndex[rf.self] = rf.state.nextIndex[rf.self] - 1
	return nil
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
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
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
		return InvalidIndex, rf.state.CurrentTerm, false
	}

	//DPrintf("Leader(Term[%d]) %d receive command: %v", rf.state.CurrentTerm, rf.self, command)

	entry := rf.addToLogCache(command)

	return entry.Index, entry.Term, true
}

func (rf *Raft) addToLogCache(command interface{}) LogEntry {
	entry := LogEntry{
		Term:  rf.state.CurrentTerm,
		Index: rf.state.nextIndex[rf.self],
		Data:  command,
	}

	// leader will increase self nextIndex and matchIndex when receive new command from up layer
	rf.state.nextIndex[rf.self]++
	rf.state.matchIndex[rf.self]++
	rf.state.Log = append(rf.state.Log, entry)
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
		rf.state.VotedFor = args.CandidateID
		rf.markAccessed()
	}

	if rf.state.CurrentTerm > args.Term {
		reply.Term = rf.state.CurrentTerm
	} else if rf.state.CurrentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	// DPrintf("server %d vote to %d, reply.Term: %d, reply.Vote: %t", rf.self, args.CandidateID, reply.Term, reply.VoteGranted)

	rf.persist()

	return
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state.CurrentTerm = newTerm
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

	reply.NextIndex = rf.state.nextIndex[rf.self]
	reply.MatchIndex = rf.state.matchIndex[rf.self]

	//DPrintf("Server(Term[%d], commitIndex[%d], nextIndex[%d], matchIndex[%d]) %d receive entries from Leader(Term[%d], LeaderCommit[%d], PrevLogIndex[%d], PrevLogTerm[%d], EntriesNum[%d]) %d",
	//	rf.state.CurrentTerm, rf.state.commitIndex, rf.state.nextIndex[rf.self], rf.state.matchIndex[rf.self],
	//	rf.self, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderID)

	// old leader's heartbeat
	if rf.state.CurrentTerm > args.Term {
		DPrintf("Current Server(term[%d], commitIndex[%d], lastApplied[%d], nextIndex[%d], log[%d]) %d Term > OldLeader(commitIndex[%d], prevLogIndex[%d]) %d",
			rf.state.CurrentTerm, rf.state.commitIndex, rf.state.lastApplied, rf.state.nextIndex[rf.self], len(rf.state.Log), rf.self, args.LeaderCommit, args.PrevLogIndex, args.LeaderID)
		// DPrintf("server %d term > old leader %d term, (%d > %d)", rf.self, args.LeaderID, rf.state.CurrentTerm, args.Term)
		reply.Term = rf.state.CurrentTerm
		reply.Success = false
		return
	}

	rf.markAccessed()
	if args.Term > rf.state.CurrentTerm {
		rf.persist()
	}
	rf.becomeFollower(args.Term)

	// current follower Log is much behind.
	if args.PrevLogIndex > reply.MatchIndex {
		DPrintf("Current Server(term[%d], commitIndex[%d], lastApplied[%d], nextIndex[%d], log[%d]) %d Log too short than Leader(commitIndex[%d], prevLogIndex[%d]) %d",
			rf.state.CurrentTerm, rf.state.commitIndex, rf.state.lastApplied,
			rf.state.nextIndex[rf.self], len(rf.state.Log), rf.self,
			args.LeaderCommit, args.PrevLogIndex, args.LeaderID)
		return
	}

	// conflict
	if rf.state.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Current Server(term[%d], commitIndex[%d], lastApplied[%d], nextIndex[%d], log[%d]) %d conflict with Leader(commitIndex[%d], prevLogIndex[%d]) %d",
			rf.state.CurrentTerm, rf.state.commitIndex, rf.state.lastApplied,
			rf.state.nextIndex[rf.self], len(rf.state.Log), rf.self,
			args.LeaderCommit, args.PrevLogIndex, args.LeaderID)

		reply.NextIndex = rf.removeConflictLog(args.PrevLogIndex-1, rf.state.Log[args.PrevLogIndex].Term)
		//reply.NextIndex = args.PrevLogIndex
		if reply.NextIndex == 0 {
			log.Printf("Server %d NextIndex: %d", rf.self, reply.NextIndex)
			panic("dangerous!!!!")
		}
		reply.MatchIndex = reply.NextIndex - 1
		reply.Success = false
		return
	}

	// after checking illegal, then do normal thing

	// no new Log entries
	if len(args.Entries) == 0 {
		if args.LeaderCommit < rf.state.commitIndex {
			// DPrintf("Leader(commitIndex[%d]) %d < Server(commitIndex[%d]) %d, maybe restart", args.LeaderID, args.LeaderCommit, rf.state.commitIndex, rf.self)
			return
		}
		rf.state.commitIndex = utils.Min(args.LeaderCommit, rf.state.matchIndex[rf.self])
		return
	}

	newMatchIndex := args.PrevLogIndex + len(args.Entries)
	newNextIndex := newMatchIndex + 1
	start := args.PrevLogIndex + 1

	for i := 0; i < len(args.Entries); i++ {
		if start+i >= len(rf.state.Log) {
			rf.state.Log = append(rf.state.Log, args.Entries[i:]...)
			break
		}
		rf.state.Log[start+i] = args.Entries[i]
	}

	rf.persist()

	if newMatchIndex >= len(rf.state.Log) {
		DPrintf("========Old ServerInfo========")
		DPrintf("ServerID: %d, ServerState: %v", rf.self, rf.state)
		DPrintf("========New ServerInfo========")
		// DPrintf("ServerID: %d, ServerState: %v", rf.self, oldState)
		DPrintf("========AppendEntriesReq========")
		DPrintf("AppendEntriesArgs: %v", args)
		panic("newMatchIndex >= logLength, dangerous!!!")
	}

	rf.state.matchIndex[rf.self] = newMatchIndex
	rf.state.nextIndex[rf.self] = newNextIndex

	reply.NextIndex = newNextIndex
	reply.MatchIndex = newMatchIndex

	if reply.NextIndex == 0 {
		log.Printf("Server %d NextIndex: %d", rf.self, reply.NextIndex)
		panic("dangerous!!!!")
	}

	if args.LeaderCommit < rf.state.commitIndex {
		// DPrintf("Leader(commitIndex[%d]) %d < Server(commitIndex[%d]) %d, maybe restart", args.LeaderID, args.LeaderCommit, rf.state.commitIndex, rf.self)
		return
	}
	rf.state.commitIndex = utils.Min(args.LeaderCommit, newMatchIndex)
	return
}

func (rf *Raft) removeConflictLog(endIndex, term int) int {
	newNextIndex := endIndex
	for newNextIndex = endIndex; newNextIndex > 0 && rf.state.Log[newNextIndex].Term == term; newNextIndex-- {
	}
	newNextIndex++

	rf.state.Log = rf.state.Log[:newNextIndex]
	rf.state.matchIndex[rf.self] = newNextIndex - 1
	rf.state.nextIndex[rf.self] = newNextIndex

	return newNextIndex
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

// 1. if CurrentTerm < peer.Term || CurrentTerm hasn't voted to any or already voted to peer
// 2. and peer's Log at least up to date as current
func (rf *Raft) tryVote(args *RequestVoteArgs) bool {
	lastLogIndex := len(rf.state.Log) - 1
	lastLogTerm := rf.state.Log[lastLogIndex].Term

	// candidate from old term or candidate's Log is too old, refused
	if rf.state.CurrentTerm > args.Term ||
		args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm &&
			args.LastLogIndex < lastLogIndex) {
		return false
	}

	// same term but already voted to others, refused
	if rf.state.CurrentTerm == args.Term &&
		rf.state.VotedFor != InitialVoteFor &&
		rf.state.VotedFor != args.CandidateID {
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

			// DPrintf("server: %d start election, CurrentTerm: %d", rf.self, requestVoteArgs.Term)

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
	requestVoteArgs := rf.genRequestVoteArgs()

	rf.persist()

	return requestVoteArgs, true
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex := len(rf.state.Log) - 1
	lastLogTerm := rf.state.Log[lastLogIndex].Term

	return &RequestVoteArgs{
		Term:         rf.state.CurrentTerm,
		CandidateID:  rf.self,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (rf *Raft) becomeCandidate() {
	rf.state.role = CandidateRole
	rf.state.CurrentTerm++
	rf.state.VotedFor = rf.self
}

func (rf *Raft) doElection(requestVoteArgs *RequestVoteArgs) {
	repliesCh := make(chan *RequestVoteReply, len(rf.peers))

	rf.sendVoteRequest(requestVoteArgs, repliesCh)
	maxTerm, voteGrantedNum := rf.tryCollectVoteReply(repliesCh)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isFollower() || maxTerm > rf.state.CurrentTerm || voteGrantedNum <= rf.halfPeer {
		rf.becomeFollower(utils.Max(maxTerm, rf.state.CurrentTerm))
		rf.persist()
		return
	}

	rf.becomeLeader()
}

// becomeLeader need lock outside
func (rf *Raft) becomeLeader() {
	rf.state.role = LeaderRole
	nextIndex := len(rf.state.Log)
	for i := range rf.state.nextIndex {
		rf.state.nextIndex[i] = nextIndex
	}

	// DPrintf("server %d become new Leader, reset nextIndex to %d, currentLog: %v", rf.self, nextIndex, rf.state.Log)
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
			if rf.state.Log[newIndex].Term == rf.state.CurrentTerm {
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

			if rf.state.commitIndex < rf.state.lastApplied {
				panic("commitIndex < lastApplied, dangerous!!!!!")
			}

			toApplyEntries := make([]ApplyMsg, 0, rf.state.commitIndex-rf.state.lastApplied)
			// toApplyEntries := make([]ApplyMsg, 0)
			for i := rf.state.lastApplied + 1; i <= rf.state.commitIndex; i++ {
				toApplyEntries = append(toApplyEntries, ApplyMsg{
					CommandValid: true,
					Command:      rf.state.Log[i].Data,
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
	go rf.heartbeat()              // leader use, send heartbeat and Log to follower
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
	// Note: maybe here current server wasn't a leader, need check again!!!
	if rf.state.role != LeaderRole {
		rf.mu.RUnlock()
		return
	}

	sendStartIndex := rf.state.nextIndex[dstServerIndex]
	prevLogIndex = sendStartIndex - 1
	prevLogTerm = rf.state.Log[prevLogIndex].Term
	logNum = len(rf.state.Log)
	currentTerm = rf.state.CurrentTerm
	toSendLog = make([]LogEntry, logNum-sendStartIndex)
	copy(toSendLog, rf.state.Log[sendStartIndex:])
	commitIndex = rf.state.commitIndex

	//DPrintf("Leader(Term[%d], commitIndex[%d], prevLogIndex[%d], prevLogTerm[%d]) %d send logEntries to follower %d, entries num: %d",
	//	currentTerm, commitIndex, prevLogIndex, prevLogTerm, rf.self, dstServerIndex, len(toSendLog))
	rf.persist()
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

	// DPrintf("Leader(Term[%d]) %d receive logEntries response from follower %d, entries num: %d", rf.state.CurrentTerm, rf.self, dstServerIndex, len(toSendLog))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state.CurrentTerm < reply.Term {
		// DPrintf("old leader(Term[%d]) %d receive higher term[%d] from server %d, become follower", rf.state.CurrentTerm, rf.self, reply.Term, dstServerIndex)
		rf.becomeFollower(reply.Term)
		rf.persist()
	}

	// do nothing
	if rf.state.role != LeaderRole {
		return
	}

	newNextIndex := utils.Min(len(rf.state.Log), reply.NextIndex)

	if !reply.Success && newNextIndex > rf.state.nextIndex[dstServerIndex] {
		return
	}

	// update follower's nextIndex and matchIndex
	rf.state.nextIndex[dstServerIndex] = newNextIndex
	rf.state.matchIndex[dstServerIndex] = newNextIndex - 1
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
			CurrentTerm: InitialTerm,
			VotedFor:    InitialVoteFor,
			commitIndex: InitialCommitIndex,
			lastApplied: InitialLastApplied,
			nextIndex:   genInitialNextIndex(len(peers)),
			matchIndex:  make([]int, len(peers)),
			Log:         genInitialLog(),
		},
		applyCh: applyCh,
	}

	raft.ctx, raft.cancelFunc = context.WithCancel(context.Background())
	return raft
}
