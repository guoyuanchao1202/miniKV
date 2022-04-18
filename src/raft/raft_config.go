package raft

const (
	DefaultTimeoutMilliseconds           = 100
	DefaultHeartbeatIntervalMilliseconds = 100
	DefaultElectionMinTimeout            = 400
	DefaultElectionMaxTimeout            = 500
	DefaultApplyMsgIntervalMilliseconds  = 20
	DefaultCheckCommitIndexInterval      = 10
)

const (
	InvalidIndex       = -1
	InitialRole        = FollowerRole
	InitialTerm        = 0
	InitialVoteFor     = -1
	InitialCommitIndex = 0
	InitialLastApplied = 0
)

func genInitialNextIndex(peerNum int) []int {
	res := make([]int, peerNum)
	for i := 0; i < peerNum; i++ {
		res[i] = 1
	}
	return res
}

func genInitialLog() []LogEntry {
	return []LogEntry{
		{
			Term:  InitialTerm,
			Index: InitialCommitIndex,
			Data:  nil,
		},
	}
}
