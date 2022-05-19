package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const Debug_masen = 0

func DPrintfM(format string, a ...interface{}) (n int, err error) {
	if Debug_masen > 0 {
		log.Printf(format, a...)
	}
	return
}
