package raft

import (
	"6.824/utils"
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


func genRandomElectionTimeout(low, high int) time.Duration {
	return time.Duration(utils.GenRandomBetween(low, high)) * time.Millisecond
}
