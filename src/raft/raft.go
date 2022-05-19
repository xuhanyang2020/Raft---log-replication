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
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	currentTerm int
	voteFor     int
	log         []Entry
	role        int // state 0 : follower   1 : candidate   2 ：leader

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	voteReceive     int
	electionTimeout int
	applyCh         chan ApplyMsg
	grantVoteBool   chan bool
	heartBeatBool   chan bool
	leaderBool      chan bool
	timer           *time.Timer
	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.

}

type Entry struct {
	Term    int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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

// return currentTerm and whether this server
// believes it is the leader.

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesResult struct {
	Term    int
	Success bool
	// Optimization
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	// Your code here (2A).
	return term, rf.role == Leader
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// current node voted for candidates with higher term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("in req RPC  %d 1 rej vote for  %d \n", rf.me, args.CandidateId)

	} else if args.Term > rf.currentTerm {
		DPrintf("id %d receives req from %d where args Term %d greater than current Term %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.role = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term

		localLastLogIndex := len(rf.log)
		// term starts from 0
		localLastLogTerm := 0
		if localLastLogIndex >= 1 {
			localLastLogTerm = rf.log[localLastLogIndex-1].Term
		}

		if args.LastLogTerm < localLastLogTerm {
			// current last log term is newer
			reply.Term = args.Term
			reply.VoteGranted = false
			DPrintf("in req RPC  %d 2 rej vote for  %d \n", rf.me, args.CandidateId)

		} else if args.LastLogTerm > localLastLogTerm {

			rf.becomeFollower(args.CandidateId, args.Term)
			rf.changeVoteBool()
			rf.voteFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			DPrintf("in req RPC  %d 1 vote for  %d \n", rf.me, args.CandidateId)
		} else {
			// compare last log index
			if args.LastLogIndex < localLastLogIndex {
				// local index newer
				reply.Term = args.Term
				reply.VoteGranted = false
				DPrintf("in req RPC  %d 3 rej vote for  %d \n", rf.me, args.CandidateId)
			} else {
				// request candidate index newer
				rf.becomeFollower(args.CandidateId, args.Term)
				rf.changeVoteBool()
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.voteFor = args.CandidateId
				DPrintf("in req RPC   %d 2 vote for  %d \n", rf.me, args.CandidateId)
			}
		}

	} else {

		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			// vote for other candidate with same term, vote exclusive
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			DPrintf("in req RPC   %d rej 4 vote for  %d \n", rf.me, args.CandidateId)
		} else if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			// not yet vote
			rf.role = Follower

			localLastLogIndex := len(rf.log)
			// term starts from 0
			localLastLogTerm := 0
			if localLastLogIndex >= 1 {
				localLastLogTerm = rf.log[localLastLogIndex-1].Term
			}

			if args.LastLogTerm < localLastLogTerm {
				// current last log term is newer
				reply.Term = args.Term
				reply.VoteGranted = false
				DPrintf("in req RPC   %d 5 rej vote for  %d \n", rf.me, args.CandidateId)

			} else if args.LastLogTerm > localLastLogTerm {
				// request candidate has newer last log index and term
				rf.becomeFollower(args.CandidateId, args.Term)
				rf.changeVoteBool()
				rf.voteFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				DPrintf("in req RPC   %d 6 vote for  %d \n", rf.me, args.CandidateId)

			} else {
				// compare last log index
				if args.LastLogIndex < localLastLogIndex {
					// local index newer
					reply.Term = args.Term
					reply.VoteGranted = false
					DPrintf("in req RPC   %d 6 rej vote for  %d \n", rf.me, args.CandidateId)
				} else {
					// request candidate index newer
					rf.becomeFollower(args.CandidateId, args.Term)
					rf.changeVoteBool()
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.voteFor = args.CandidateId
					DPrintf("in req RPC   %d 7 vote for  %d \n", rf.me, args.CandidateId)
				}
			}
		}
	}

	rf.mu.Unlock()

	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
}

func (rf *Raft) initRequestVote() {
	DPrintf("%d entering init req func \n", rf.me)
	rf.mu.Lock()
	currentRole := rf.role
	currentTerm := rf.currentTerm
	// if is follower or leader, return
	if currentRole != Candidate {
		rf.mu.Unlock()
		return
	}
	// leaderFlag flag if already becomes leader
	leaderFlag := false

	localLastLogIndex := len(rf.log)
	// term starts from 0
	localLastLogTerm := 0
	if localLastLogIndex >= 1 {
		localLastLogTerm = rf.log[localLastLogIndex-1].Term
	}
	// DPrintf("init req 1")
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		// each gorountine responsible for calling RPC to a peer
		go func(peerIndex int) {
			if peerIndex == rf.me {
				return // one gorountine return
			}
			// DPrintf("init req 2")
			request := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: localLastLogIndex,
				LastLogTerm:  localLastLogTerm,
			}
			// DPrintf("init req 3")
			responce := RequestVoteReply{}
			DPrintf("%d send request vote to %d \n", rf.me, peerIndex)
			getResp := rf.sendRequestVote(peerIndex, &request, &responce)
			DPrintf("%d receive reply vote from %d \n", rf.me, peerIndex)
			// DPrintf("init req 5")
			if getResp {
				// there exists candidate or leader with higher term, become follower and listen to heartbeat
				if responce.Term > currentTerm {
					// waiting for heartbeat to update the leader
					rf.becomeFollower(-1, responce.Term)
					return
				}
				// if already becomes follower, current goroutine return
				if rf.role != Candidate {
					return
				}
				// DPrintf("init req 4")
				if responce.VoteGranted {
					rf.mu.Lock()
					rf.voteReceive += 1
					DPrintf("id %d voteReceive is %d \n", rf.me, rf.voteReceive)
					if !leaderFlag && rf.voteReceive > len(rf.peers)/2 {
						leaderFlag = true
						DPrintf("%d becomes a leader \n", rf.me)
						rf.changeLeaderBool()
						// rf.leaderBool <- true
						// go func() {
						// 	select {
						// 	case <-rf.leaderBool:
						// 		fmt.Println("leaderbool")
						// 	default:
						// 		fmt.Println("nothing in leader channel")
						// 	}
						// }()
						rf.becomeLeader()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			}
		}(i)

	}
}

func (rf *Raft) initRequestVoteToPeer() {

}

func (rf *Raft) becomeCandidate() {
	// DPrintfM("become a candidate, my id is %d , new term is %d", rf.me, rf.currentTerm+1)
	rf.role = Candidate
	rf.voteFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	rf.voteReceive = 1 // vote for itself
	rf.electionTimeout = rand.Intn(1500) + 150
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
}

func (rf *Raft) becomeLeader() {
	// DPrintfM("become a leader, my id is %d \n", rf.me)
	rf.role = Leader
	lastIndex := len(rf.log) + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}
	// DPrintfM("nextIndex list: %v \n", rf.nextIndex)
}

func (rf *Raft) becomeFollower(voteFor int, termSet int) {
	// DPrintfM("id %d become a follower, vote for %d, term is %d", rf.me, voteFor, termSet)
	// DPrintf("%d", rf.me)
	rf.role = Follower
	rf.voteFor = voteFor
	rf.currentTerm = termSet
}

func (rf *Raft) changeHeartBeatBool() {
	DPrintf("%d set heartbeat true", rf.me)
	go func() {
		select {
		case <-rf.heartBeatBool:
			DPrintf("%d has heartbeat true", rf.me)
		default:
		}
		rf.heartBeatBool <- true
		DPrintf("%d set heartbeat true", rf.me)
	}()
}

func (rf *Raft) changeVoteBool() {
	go func() {
		select {
		case <-rf.grantVoteBool:
		default:
		}
		rf.grantVoteBool <- true
	}()
}

func (rf *Raft) changeLeaderBool() {
	go func() {
		select {
		case <-rf.leaderBool:
		default:
		}
		rf.leaderBool <- true
	}()
}

func (rf *Raft) resetTimer() {

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
	// DPrintf("send req vote 1")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// DPrintf("send req vote 2")
	return ok
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesRequest, resp *AppendEntriesResult) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
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
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).
	if isLeader {
		rf.log = append(rf.log, Entry{
			Term:    term,
			Command: command,
		})
		index = len(rf.log)
		// DPrintfM("%d append log: %v \n", rf.me, rf.log)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	// DPrintf("start making a new node")
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.leaderBool = make(chan bool)
	rf.heartBeatBool = make(chan bool)
	rf.grantVoteBool = make(chan bool)

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = []Entry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// becomes a follower first
	rf.becomeFollower(-1, rf.currentTerm)
	// DPrintf("make0")
	rf.voteReceive = 0
	rf.electionTimeout = rand.Intn(150) + 150
	// fmt.Printf("time out is %d ms", rf.electionTimeout)
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)

	// rf.changeHeartBeatBool()
	// rf.changeLeaderBool()
	// rf.changeVoteBool()
	// fmt.Println("make2")
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	go func() {
		for {
			if rf.killed() {
				return
			}
			// DPrintf("loop")
			rf.mu.Lock()
			currentRole := rf.role
			// DPrintf("%d current role %d \n", rf.me, currentRole)
			rf.mu.Unlock()
			switch currentRole {
			case Follower:
				rf.mu.Lock()
				DPrintf("I'm a follower %d , voted for %d \n", rf.me, rf.voteFor)
				rf.electionTimeout = rand.Intn(150) + 150
				rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				rf.mu.Unlock()
				// other nodes RPC calling
				select {
				case <-rf.timer.C:
					// timeout, convert to a candidate
					rf.mu.Lock()
					// DPrintfM("follower timeout, follower id is %d \n", rf.me)
					rf.becomeCandidate()
					rf.mu.Unlock()
					// DPrintf(rf.role)

				case <-rf.grantVoteBool:
					DPrintf("id %d vote for %d \n", rf.me, rf.voteFor)
					// voted for other nodes, continue to next update loop
				case <-rf.heartBeatBool:
					DPrintf("%d received a heartbeat", rf.me)
					DPrintf("listening to heartbeat")
					// get heartbeats from other nodes, continue to next update loop
				case <-rf.leaderBool:
					DPrintf("follower %d becomes a leader \n", rf.me)
					// becomes a leader
				}
			case Candidate:
				go rf.initRequestVote()
				DPrintf("I'm a candidate %d \n", rf.me)
				select {
				case <-rf.leaderBool:
					// has been leader
				case <-rf.timer.C:
					// split votes node
					rf.mu.Lock()
					rf.becomeCandidate()
					rf.mu.Unlock()
				case <-rf.grantVoteBool:
					// voted for others
					// rf.mu.Lock()
					// rf.becomeFollower(-1, rf.currentTerm)
					// rf.mu.Unlock()
				case <-rf.heartBeatBool:
					rf.mu.Lock()
					rf.becomeFollower(-1, rf.currentTerm)
					rf.mu.Unlock()
				}
			case Leader:
				DPrintf("I'm a leader, my id is %d", rf.me)
				rf.initAppendEntries()
				// DPrintf("I'm a leader, my id is %d, currrent Term %d \n", rf.me, rf.currentTerm)
			default:
				continue
			}
		}
	}()
	return rf
}

// RPC struct for appending entries
func (rf *Raft) AppendEntries(request *AppendEntriesRequest, responce *AppendEntriesResult) {
	rf.mu.Lock()
	DPrintf("%d call me %d RPC, Term %d, PrefLogIndex %d, PrevLogTerm %d, LeaderCommit %d", request.LeaderId, rf.me, request.Term, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit)
	if request.Term < rf.currentTerm {
		DPrintf("who %d request term is %d less than current term %d", request.LeaderId, request.Term, rf.currentTerm)
		responce.Term = rf.currentTerm
		responce.Success = false
		rf.mu.Unlock()
		return
	} else {
		DPrintf("%d request term is %d equals to current term %d", request.LeaderId, request.Term, rf.currentTerm)
		rf.changeHeartBeatBool()
		rf.becomeFollower(request.LeaderId, request.Term)
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
		if request.PrevLogIndex < 1 {
			// entries from start
			responce.Term = rf.currentTerm
			responce.Success = true
			rf.log = append(rf.log[:request.PrevLogIndex], request.Entries...)
			if len(request.Entries) > 0 {
				// DPrintfM("append entries %v", request.Entries)
			}
			// // DPrintfM("%d append entries %d \n", rf.me, len(rf.log))
			if request.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(request.LeaderCommit), float64(len(rf.log)-1)))
			}
			rf.applyLogs()
		} else if request.PrevLogIndex > len(rf.log) {
			responce.Term = rf.currentTerm
			responce.Success = false
			responce.ConflictIndex = len(rf.log)
			responce.ConflictTerm = -1
		} else if rf.log[request.PrevLogIndex-1].Term != request.PrevLogTerm {
			// // DPrintfM("Append Entries: prevLogIndex %d, log %d \n", request.PrevLogIndex, len(rf.log))
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			// masen TODO: 这里不需要删，下面append后会自动删除
			// rf.log = rf.log[:request.PrevLogIndex]
			responce.Term = rf.currentTerm
			responce.Success = false
			responce.ConflictTerm = rf.log[request.PrevLogIndex-1].Term
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term == responce.ConflictTerm {
					responce.ConflictIndex = i
					break
				}
			}
		} else {
			responce.Term = rf.currentTerm
			responce.Success = true
			// Append any new entries not already in the log
			// if len(request.Entries)+request.PrevLogIndex > len(rf.log) {
			rf.log = append(rf.log[:request.PrevLogIndex], request.Entries...)
			// } else {
			// 	// masen TODO: 可能需要特殊判读
			// }
			if request.LeaderCommit > rf.commitIndex {
				// // DPrintfM("%d commitIndex %d, leaderCommit %d", rf.me, rf.commitIndex, request.LeaderCommit)
				rf.commitIndex = int(math.Min(float64(request.LeaderCommit), float64(len(rf.log))))
			}
			rf.applyLogs()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) initAppendEntries() {
	for {
		if rf.killed() {
			return
		}
		DPrintf("enter init append entries loop %d", rf.me)
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		// localLastLogIndex := len(rf.log)
		// // term starts from 0
		// localLastLogTerm := 0
		// if localLastLogIndex >= 1 {
		// 	localLastLogTerm = rf.log[localLastLogIndex-1].Term
		// }
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			// DPrintf("%d in a new loop to init append entries", rf.me)
			go func(peerIndex int) {
				if peerIndex == rf.me {
					// rf.mu.Unlock()
					return
				}

				for {
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					prevLogIndex := rf.nextIndex[peerIndex] - 1
					prevLogTerm := 0
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex-1].Term
					}
					entries := make([]Entry, 0)
					if rf.nextIndex[peerIndex] <= len(rf.log) {
						entries = append([]Entry{}, rf.log[rf.nextIndex[peerIndex]-1:]...)
					}
					appendRequest := AppendEntriesRequest{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					appendResult := AppendEntriesResult{}
					// fmt.Printf("init append2fafafaf %d \n", appendRequest.LeaderId)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(peerIndex, &appendRequest, &appendResult)

					// fmt.Printf("init append3 %d \n", rf.me)
					if ok {
						// DPrintfM("%d send heartbeat to %d, nextIndex %d, log size %d \n", rf.me, peerIndex, rf.nextIndex[peerIndex], len(rf.log))
						// // DPrintfM("%d prepare send heartbeat to %d, nextIndex %d, log size %d \n", rf.me, peerIndex, rf.nextIndex[peerIndex], len(rf.log))
						rf.mu.Lock()
						if appendResult.Term > rf.currentTerm {
							rf.becomeFollower(-1, appendResult.Term)
							rf.mu.Unlock()
							return
						}
						// other goroutines will not go
						if rf.role != Leader || rf.currentTerm != appendRequest.Term {
							rf.mu.Unlock()
							return
						}

						if appendResult.Success {
							rf.matchIndex[peerIndex] = prevLogIndex + len(entries)
							tmp := rf.nextIndex[peerIndex]
							rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
							if tmp != rf.nextIndex[peerIndex] {
								// DPrintfM("%d update nextIndex from %d to %d \n", peerIndex, tmp, rf.nextIndex[peerIndex])
							}

							copy_matchIndex := make([]int, len(rf.matchIndex))
							copy(copy_matchIndex, rf.matchIndex)
							copy_matchIndex[rf.me] = len(rf.log)
							sort.Ints(copy_matchIndex)
							if tmp != rf.nextIndex[peerIndex] {
								// DPrintfM("matchIndex %v \n", copy_matchIndex)
							}
							N := copy_matchIndex[len(copy_matchIndex)/2]
							// if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
							// and log[N].term == currentTerm: set commitIndex = N
							if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
								// DPrintfM("%d success nextIndex %d, log size %d \n", peerIndex, rf.nextIndex[peerIndex], len(rf.log))
								rf.commitIndex = N
								rf.applyLogs()
							}
							rf.mu.Unlock()
							return
						} else {
							// optimization: When rejecting an AppendEntries request,
							// the follower can include the term of the conflicting entry and the first index it stores for that term.
							// The leader can decrement nextIndex to bypass all of the conflicting entries in that term.
							found_same_term := false
							for j := 0; j < len(rf.log); j++ {
								if rf.log[j].Term == appendResult.ConflictTerm {
									found_same_term = true
								}
								if rf.log[j].Term > appendResult.ConflictTerm {
									if found_same_term {
										rf.nextIndex[peerIndex] = j
									} else {
										rf.nextIndex[peerIndex] = appendResult.ConflictIndex
									}
									break
								}
							}
							if rf.nextIndex[peerIndex] < 1 {
								rf.nextIndex[peerIndex] = 1
							}
							// DPrintfM("%d fair nextIndex %d, log size %d \n", peerIndex, rf.nextIndex[peerIndex], len(rf.log))
						}
					} else {
						// network error
						return
					}
					rf.mu.Unlock()
				}
			}(i)
			// DPrintf("%d finish a loop to init append entries", rf.me)
		}
		time.Sleep(100 * time.Millisecond) // heartbeat interval
	}
}

// apply log to state machine
func (rf *Raft) applyLogs() {
	// // DPrintfM("%d apply logs, commitIndex %d, lastApplied %d \n", rf.me, rf.commitIndex, rf.lastApplied)
	for rf.lastApplied < rf.commitIndex {
		// DPrintfM("%d apply log %v \n", rf.me, rf.log[rf.lastApplied])
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied-1].Command,
		}
	}
}
