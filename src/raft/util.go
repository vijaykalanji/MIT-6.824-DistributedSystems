package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 0
const ColorByID = 1

const (
	nocolor   = 0
	red       = 31
	green     = 32
	yellow    = 33
	blue      = 34
	pink      = 35
	turquoise = 36
	gray      = 37
)

var (
	baseTimestamp time.Time
)

func init() {
	baseTimestamp = time.Now()
}

func Dprintf(id int, state string, format string, a ...interface{}) {
	if Debug > 0 {
		var color int
		var stateString string
		switch state {
		case Follower:
			color = blue
			stateString = "FOLLOWER"
		case Candidate:
			color = yellow
			stateString = "CANDIDATE"
		case Leader:
			color = red
			stateString = "LEADER"
		default:
			color = gray
			stateString = "TESTER"
		}
		if ColorByID > 0 {
			switch id {
			case 0:
				color = red
			case 1:
				color = green
			case 2:
				color = blue
			case 3:
				color = yellow
			case 4:
				color = pink
			}
		}
		fmt.Printf("[%04d] \x1b[%dm%-9s [id=%d]\x1b[0m %s\n", int(time.Now().Sub(baseTimestamp)/time.Millisecond), color, stateString, id, fmt.Sprintf(format, a...))
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
