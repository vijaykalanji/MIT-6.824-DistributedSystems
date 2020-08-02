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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	///Persistent state on all servers.

	///Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	///candidateId that received vote in current term (or null if none)
	votedFor int
	///log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log []LogEntry

	////Volatile state on all servers
	///index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	///index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	////Volatile state on leaders:
	///for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	///for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int

	newEntryCh chan int // frank's suggestion

	currentState  string
	electionTimer *time.Timer
	applyCh       chan ApplyMsg
}

type RaftPersistenceObject struct {
	CurrentTerm int
	Log         []LogEntry
	VotedFor    int
}

///Each entry contains the term in which it was  created (the number in each box) and a command for the state  machine. An entry is considered committed if it is safe for that entry to be applied to state machines.
type LogEntry struct {
	Term     int
	LogIndex int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	////rf.debug("In GetState() currentTerm = %d, currentState =%t ", rf.currentTerm, rf.currentState == Leader)
	return rf.currentTerm, rf.currentState == Leader
}

const (
	Follower  = "FOLLOWER"
	Candidate = "CANDIDATE"
	Leader    = "LEADER"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistenceObject{
			CurrentTerm: rf.currentTerm,
			Log:         rf.log,
			VotedFor:    rf.votedFor,
		})

	rf.debug("Persisted RAFT internal data currentTerm = %d Log contents = %#v  VotedFor = %d Length of the data = %d", rf.currentTerm, rf.log, rf.votedFor, buf.Len())
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := RaftPersistenceObject{}
	d.Decode(&obj)
	rf.votedFor, rf.currentTerm, rf.log = obj.VotedFor, obj.CurrentTerm, obj.Log
	rf.debug("Read persisted data from the backup  currentTerm = %d Log contents = %#v  VotedFor = %d Length of the data = %d", rf.currentTerm, rf.log, rf.votedFor, len(data))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!v
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//candidate’s term
	Term int
	//candidate requesting vote
	CandidateId int
	//index of candidate’s last log entry
	LastLogIndex int
	//term of candidate’s last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term             int
	LeaderID         int
	PreviousLogTerm  int
	PreviousLogIndex int
	LogEntries       []LogEntry
	LeaderCommit     int
}

type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.debug("***************Inside RequestVote Receiver Handler *********************")
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.debug("REQUEST FROM CANDIDATE = %#v My rf.currentTerm = %d rf.votedFor = %d", args, rf.currentTerm, rf.votedFor)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	/// If the candidate is asking vote for a higher term, then first reset the voted to -1
	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkWhetherLogIsUpToDate(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	rf.persist()
}

func (rf *Raft) checkWhetherLogIsUpToDate(args *RequestVoteArgs) bool {
	lastIndex, lastTerm := rf.getLastEntryInfo()
	rf.debug("~~~~~~~~~~~~~~~~~~~~Inside checkWhetherLogIsUpToDate() lastIndex = %d  lastTerm =%d args.LastLogTerm = %d args.LastLogIndex =%d",
		lastIndex, lastTerm, args.LastLogTerm, args.LastLogIndex)
	if lastTerm == args.LastLogTerm {
		return lastIndex <= args.LastLogIndex
	}
	return lastTerm < args.LastLogTerm
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
// A false return can be caused by a dead server, a live server that
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

//
// the service using Raft (e.g. a k/v server) wants to start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != Leader {
		return index, term, false
	}
	newEntryIndex := rf.log[len(rf.log)-1].LogIndex + 1
	term = rf.currentTerm
	// Attach this log entry to the local log first. Then replicate this entry to
	// all other peers.
	currentLogEntry := LogEntry{
		LogIndex: newEntryIndex,
		Command:  command,
		Term:     rf.currentTerm,
	}
	rf.debug(" @@@@@@@@@@@@@@@@@@@@@@@@@@@ NEW COMMAND TO ADD TO LOG @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ %#v", currentLogEntry)
	rf.log = append(rf.log, currentLogEntry)
	// frank's suggestion:
	go func() {
		rf.newEntryCh <- newEntryIndex

	}()
	// go rf.sendAppendEntries()
	return newEntryIndex, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me
	rf.electionTimer = time.NewTimer((400 + time.Duration(rand.Intn(300))) * time.Millisecond)
	/// Initialized to 0 on first boot, increases monotonically (From the paper)
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.votedFor = -1
	rf.currentState = Follower
	// Your initialization code here (2A, 2B, 2C).
	////rf.debug("Inside Make Method %d", me)
	// initialize from state persisted before a crash
	dummyLogEntry := LogEntry{
		LogIndex: 0,
		Command:  nil,
		Term:     0,
	}
	rf.log = append(rf.log, dummyLogEntry)
	rf.readPersist(persister.ReadRaftState())
	go rf.waitForElectionTimerToGoOff()
	return rf
}

/// This method waits for the timer to go off. It will always wait on this timer and will initiate a new election
/// when the timer goes off.
func (rf *Raft) waitForElectionTimerToGoOff() {
	for {
		<-rf.electionTimer.C
		////rf.debug("+++++Conducting an election")
		//Let us reset the timer here itself. This way when we don't get a majority we will save some time
		rf.resetElectionTimer()
		go rf.conductElection()
	}
}

func (rf *Raft) conductElection() {
	// Spawn off go routines.
	// Collect the votes.
	rf.transitionToCandidate()
	rf.debug("################	Conducting election TERM = %d	################", rf.currentTerm+1)
	lastIndex, lastTerm := rf.getLastEntryInfo()
	requestVoteArgs := RequestVoteArgs{Term: rf.currentTerm + 1, CandidateId: rf.me, LastLogTerm: lastTerm, LastLogIndex: lastIndex}
	// Send the request to all the peers.
	/// voteCount is initialized to 1 indicated this peer has voted for itself.
	var voteCount = 1
	count := len(rf.peers) - 1 //Count of peers. I should receive these many votes.
	rf.debug(" Count is  %d ", voteCount)
	///Before performing any action check whether the peer is still in the Leader state.
	if rf.currentState != Leader && rf.currentState != Follower {
		//This channel will collect responses from other peers.
		votesCh := make(chan bool)
		for id := range rf.peers {
			if id != rf.me {
				//rf.debug("Inside Go routine",id)
				go func(id int, peer *labrpc.ClientEnd) {
					requestVoteReply := RequestVoteReply{}
					rf.debug(" Before sending the request vote to %d ", id)
					ok := rf.sendRequestVote(id, &requestVoteArgs, &requestVoteReply)
					response := ok && requestVoteReply.VoteGranted
					rf.debug(" The value of OK= %t and VoteGranted = %t ", ok, requestVoteReply.VoteGranted)
					//Check now whether everything is OK. This is moved from outside as we are creating requestVoteReply within
					// Go routine.
					if requestVoteReply.Term > rf.currentTerm {
						rf.debug("Got a higher current term from peer %d ,So breaking requestVoteReply.Term = %d rf.currentTerm = %d", rf.me, requestVoteReply.Term, rf.currentTerm)
						rf.transitionToFollower(requestVoteReply.Term)
					}
					votesCh <- response
				}(id, rf.peers[id])
			}
		}
		///Collect the responses from all the peers.
		for {
			///When the count becomes 0 , all the peers have responded.
			if count == 0 {
				rf.debug("Count == 0")
				break
			}
			hasPeerVotedForMe := <-votesCh
			///Decrement the count as and when you hear from a peer.
			count--
			/// This means that some peer has claimed the leadership. Time to relegate to follower.
			if rf.currentState == Follower {
				break
			}
			rf.debug("Did I receive vote ? --> %t, currentVoteCount =%d ", hasPeerVotedForMe, voteCount)
			if hasPeerVotedForMe {
				voteCount += 1
				//rf.debug(rf.me ," Incremented vote count-->",voteCount)
				//rf.debug()
				if voteCount > (len(rf.peers) / 2) {
					//rf.debug("I won the election !!! ",rf.me,"Vote count -->",voteCount, " ",len(rf.peers)/2)
					rf.debug("I won the election !!! VoteCount=%d, threshold = %d", voteCount, len(rf.peers)/2)
					go rf.promoteToLeader()
					break
				}

			}
		}
	}
	rf.persist()
}
func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return len(rf.log) - 1, entry.Term
	}
	return 0, 0
}
func (rf *Raft) transitionToCandidate() {
	rf.currentState = Candidate
	//rf.debug("BEFORE Transition to candidate, term=%d", rf.currentTerm)
	rf.votedFor = rf.me
	////rf.debug("Transition to candidate, term=%d", rf.currentTerm)
}
func (rf *Raft) transitionToFollower(newTerm int) {
	follower := Follower
	rf.currentState = follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.resetElectionTimer()

}

func (rf *Raft) resetElectionTimer() {
	rf.debug("Resetting my election timer.")
	rf.electionTimer.Stop()
	rf.electionTimer.Reset((400 + time.Duration(rand.Intn(300))) * time.Millisecond)
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	///When I am the leader, I don't expect to receive any append entries request. Hence the timer is stopped.
	///If I receive an append entry request with a higher term then, I will have to transition to follower and restart the timer.
	rf.electionTimer.Stop()
	rf.debug("Promoting myself as LEADER")
	rf.currentTerm = rf.currentTerm + 1
	rf.currentState = Leader
	//Paper: (Reinitialized after election)
	//nextIndex[] for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	//matchIndex[] for each server, index of highest log entry
	//known to be replicated on server
	//(initialized to 0, increases monotonically)
	lastLogEntryIndex := rf.getIndexOfLastLogEntry()
	for index, _ := range rf.nextIndex {
		rf.nextIndex[index] = lastLogEntryIndex + 1
		rf.matchIndex[index] = 0
	}
	rf.mu.Unlock()
	rf.sendAppendEntries()
}

/// This method serves two purposes. If there are entries that are to be sent to peers, it will send the entries.
/// If there are no such entries then it will send a heart beat (Null entry).
/// I treat each peer independently. So, we consult the array nextIndex to see how many entries have we sent already for a peer.
func (rf *Raft) sendAppendEntries() {
	rf.debug("Inside sendAppendEntries")
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		// frank's suggestion:
		select {
		case <-ticker.C:
		case <-rf.newEntryCh:
		}
		/// First check whether this peer is still the leader. If not stop everything.
		//rf.debug("Try to get the lock ")
		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			break
		}
		//rf.debug("Before releasing the  acquired the lock ")
		rf.mu.Unlock()
		/// For each of the peer we shall send a heart beat / append entry.
		for peer := range rf.peers {
			if peer != rf.me {
				go func(peerId int) {
					indexOfLastLogEntry := rf.getIndexOfLastLogEntry() + 1
					prevLogIndex, prevLogTerm := rf.getPrevLogDetails(peerId)
					rf.debug(" Inside sendAppendEntries(). Peer id = %d indexOfLastLogEntry of the logM = %d , rf.nextIndex[peerId] ===> %d", peerId, indexOfLastLogEntry, rf.nextIndex[peerId])
					logEntrySize := indexOfLastLogEntry - rf.nextIndex[peerId]
					logEntries := make([]LogEntry, logEntrySize)
					copy(logEntries, rf.log[rf.nextIndex[peerId]:])
					reply := AppendEntriesReply{}
					args := AppendEntriesArgs{
						Term:             rf.currentTerm,
						LeaderID:         rf.me,
						PreviousLogIndex: prevLogIndex,
						PreviousLogTerm:  prevLogTerm,
						LogEntries:       logEntries,
						LeaderCommit:     rf.commitIndex,
					}
					requestName := "Raft.AppendEntries"
					ok := rf.peers[peerId].Call(requestName, &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.debug("Stepping down from being a LEADER because peeer %d response was ===> %#v \n", peerId, reply)
						rf.transitionToFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					if ok && reply.Success {
						//Update the nextIndex for this peer.
						rf.debug("Peer %d accepted the append entries . Response was ===> %#v \n", peerId, reply)
						rf.nextIndex[peerId] = rf.nextIndex[peerId] + len(logEntries)
						rf.debug("UPDATING THE matchIndex for Peer %d  %#v \n", peerId, rf.matchIndex)
						rf.matchIndex[peerId] = prevLogIndex + logEntrySize
					} else {
						rf.debug("Peer %d did not accept append entries. Response was ===> %#v \n", peerId, reply)
						if reply.NextIndex > 0 {
							rf.debug("Decrementing the nextIndex of %d  peer to  ===> %d \n", peerId, rf.nextIndex[peerId]-1)
							rf.nextIndex[peerId] = rf.nextIndex[peerId] - 1
						}
					}

				}(peer)
			}
		}
		rf.moveCommitIndex()
		//rf.debug("Resetting election timer at the end")
		//rf.resetElectionTimer()
		//Check at the last. This is because this way the first HB will be sent immediately.
		//timer := time.NewTimer(100 * time.Millisecond)
		//<-timer.C
	}
	rf.persist()
}

func (rf *Raft) moveCommitIndex() {
	//rf.debug("Acquiring lock as part of moveCommitIndex ()")
	rf.mu.Lock()
	//rf.debug("Acquired lock as part of moveCommitIndex () +++++++++++++++++")
	defer rf.mu.Unlock()
	rf.debug("Inside moveCommitIndex () +++++++++++++++++My commitIndex = %d  log length = %d ", rf.commitIndex, len(rf.log))
	for i := rf.commitIndex; i < len(rf.log); i++ {
		majorityAgreed := 1 // Leader always agrees with itself.
		if i == 0 {         // Position
			continue
		}
		for j := 0; j < len(rf.matchIndex); j++ {
			if rf.matchIndex[j] >= i {
				majorityAgreed++
			}
		}
		rf.debug("CHECKING WHETHER Majority Agreed  %d MATCH INDEX %#v COMMIT INDEX %d ", majorityAgreed, rf.matchIndex, rf.commitIndex)
		if majorityAgreed > (len(rf.peers) / 2) {
			rf.persist()
			rf.commitIndex++
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			rf.debug("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& LEADER Applied this message to channel  %d     %v &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&", i, rf.log[i].Command)
			rf.mu.Lock()
		}
		//rf.debug("AFTER  Trying to apply this message to channel ++++++++++++++++++++++++++++++++++++++++++++++")
	}
	//rf.debug("Releasing  lock as part of moveCommitIndex ()")
}

// This is the receiver for append entries.

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("Received an APPEND ENTRY/HEART BEAT. PROCESSING")
	rf.debug("AppendEntries: from LEADER %#v \n", args)
	rf.debug("My current state: %#v \n", rf)
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the exis	ting entry and all that
	//follow it (§5.3)
	//4. Append any new entries not already in the log
	//5. If leaderCommit > commitIndex, set commitIndex =
	//	min(leaderCommit, index of last new entry)
	/////////////Pending implementation point 5 above.
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	////rf.debug("Update my term to that of the leaders %d", args.Term)
	rf.currentTerm = args.Term
	////rf.debug("Dereferencing %d length of the my log =>", len(rf.log)-1)
	////rf.debug("Current log contents for this peer => %v ", rf.log)
	// Check first whether it is a heartbeat or an actual append entry.
	// If it is heartbeat, then just reset the timer and then go back.
	//Otherwise, we need to add the entries into the logs of this peer.
	// If this is heart beat, then we know that the command is going to be nil.
	// Identify this and return.
	lastLogEntryIndex := len(rf.log) - 1
	rf.debug("lastLogEntryIndex %d", lastLogEntryIndex)
	//If we have reached this point then the request has to be one of heart beat or append entries.
	reply.Term = rf.currentTerm
	var lastEntry LogEntry
	rf.debug("RF LOG  %#v    args.PreviousLogIndex = %d", rf.log, args.PreviousLogIndex)
	if len(rf.log) > args.PreviousLogIndex {
		lastEntry = rf.log[args.PreviousLogIndex]
	} else {
		reply.Success = false
		reply.NextIndex = len(rf.log)
		rf.debug("I did not agree with AppendEntries. Setting nextIndex to %d and returning FALSE", reply.NextIndex)
		return
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	if lastEntry.Term != args.PreviousLogTerm {
		rf.debug("**FALSE**: 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3). Set the reply.NextIndex to %d ", reply.NextIndex)
		rf.debug("Setting the nextIndex for this peer to be %d. The contents of the logs  = %#v ", len(rf.log), rf.log)
		reply.Success = false
		reply.Term = lastEntry.Term
		reply.NextIndex = len(rf.log)
		rf.debug("The contents of the reply to the leader  %#v ", reply)
		return
	}
	//As a first step we will reset the election timer.
	rf.resetElectionTimer()
	//3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	//Clean up everything to the right of matched index
	rf.log = rf.log[:args.PreviousLogIndex+1]
	rf.log = append(rf.log, args.LogEntries...)

	//}
	if args.LeaderCommit > rf.commitIndex {
		rf.debug("(5) Update commitIndex. LeaderCommit %v  rf.commitIndex %v \n", args.LeaderCommit, rf.commitIndex)
		//Check whether all the entries are committed prior to this.
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, lastLogEntryIndex+1)
		rf.debug("moving ci from %v to %v", oldCommitIndex, rf.commitIndex)
		go func() {
			//Send all the received entries into the channel
			j := 0
			rf.debug("OLD COMMIT INDEX %d, LEADER COMMIT %d  ", oldCommitIndex, args.LeaderCommit)
			for i := oldCommitIndex; i < args.LeaderCommit && i < len(rf.log); i++ {
				if i == 0 { // Position
					continue
				}
				rf.debug("Committing %v ", i)
				applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
				j++
				rf.debug(" &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& PEER Sent a response to Apply Channel  %v  &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&", applyMsg)
				rf.applyCh <- applyMsg
			}
		}()
	}
	reply.Success = true
	//Check at the last. This is because this way the first HB will be sent immediately.
	//timer := time.NewTimer(100 * time.Millisecond)
	rf.persist()
}

func (rf *Raft) getIndexOfLastLogEntry() int {
	return len(rf.log) - 1
}
func (rf *Raft) getPrevLogDetails(peerId int) (int, int) {
	nextIndex := rf.nextIndex[peerId]
	if nextIndex > 0 {
		prevLog := rf.log[nextIndex-1]
		return nextIndex - 1, prevLog.Term
	} else {
		return 0, 0
	}
}

func (rf *Raft) debug(format string, a ...interface{}) {
	// NOTE: must hold lock when this function is called!
	Dprintf(rf.me, rf.currentState, format, a...)
	return
}

func (rf *Raft) printSlice(s []LogEntry, str string) {
	rf.debug("%s -->", str)
	rf.debug(" length=%d capacity=%d %v\n", len(s), cap(s), s)
}

//This helper method returns the number of log entries that are to be sent to the peer from the leader.
func (rf *Raft) getLogEntriesForPeer(peerId int) []LogEntry {
	return nil
}
