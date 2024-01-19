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
	//	"bytes"

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
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

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role          Role
	currentTerm   int
	votedFor      int
	lastHeartbeat time.Time
	log           []Entry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = 0
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dInfo, "S%d[T:%d] receive voteRequest from S%d[T:%d]", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		Debug(dInfo, "S%d[T:%d] becomes follower", rf.me, rf.currentTerm)
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// If currentTerm equal to args.Term
	if rf.role == Leader || rf.role == Candidate {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = rf.currentTerm
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
			Debug(dVote, "S%d[T:%d] -> Grant vote to S%d", rf.me, rf.currentTerm, args.CandidateId)
		} else {
			reply.VoteGranted = false
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor, rf.role = args.Term, -1, Follower
		Debug(dInfo, "S%d T:%d becomes follower", rf.me, rf.currentTerm)
	} else if args.Term < rf.currentTerm {
		reply.Success, reply.Term, reply.NextIndex = false, rf.currentTerm, -1
		return
	}
	if args.Term == rf.currentTerm {
		rf.lastHeartbeat = time.Now()
		rf.role = Follower
		if args.PrevLogIndex > len(rf.log)-1 {
			reply.Success, reply.Term, reply.NextIndex = false, rf.currentTerm, len(rf.log)-1
			Debug(dLog, "S%d T:%d logReplication failed: Can not find PrevLogIndex(%d) in log(length: %d)", rf.me, rf.currentTerm, args.PrevLogIndex, len(rf.log))
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			nextIndex := args.PrevLogIndex
			for nextIndex > 1 && rf.log[nextIndex].Term == rf.log[args.PrevLogIndex].Term {
				nextIndex--
			}
			reply.Success, reply.Term, reply.NextIndex = false, rf.currentTerm, nextIndex
			Debug(dLog, "S%d T:%d logReplication failed: Log term incorrect: %d-%d", rf.me, rf.currentTerm, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			return
		}
		for i := range args.Entries {
			if args.PrevLogIndex+1+i > len(rf.log)-1 {
				rf.log = append(rf.log, args.Entries[i])
			} else {
				rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
			}
		}
		reply.Success, reply.Term, reply.NextIndex = true, rf.currentTerm, -1
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	index = len(rf.log)
	rf.log = append(rf.log, Entry{
		Command: command,
		Term:    rf.currentTerm,
	})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replicatorCond[i].Signal()
	}
	term = rf.currentTerm
	isLeader = rf.role == Leader
	rf.matchIndex[rf.me] = len(rf.log) - 1
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	var duration int64
	for !rf.killed() {
		// Your code here (2A)
		// pause for a random amount of time between 320 and 520
		// milliseconds.
		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()
		duration = 320 + (rand.Int63() % 200)
		time.Sleep(time.Duration(duration)*time.Millisecond - time.Since(lastHeartbeat))
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader {
			elapsedTime := time.Since(rf.lastHeartbeat)
			rf.mu.Unlock()
			// timeout
			if elapsedTime >= time.Duration(duration)*time.Millisecond {
				rf.mu.Lock()
				rf.role = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.lastHeartbeat = time.Now()
				go rf.startElection(rf.currentTerm)
				Debug(dVote, "S%d T:%d start election", rf.me, rf.currentTerm)
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

func (rf *Raft) startElection(currentTerm int) {
	var votes int = 1
	var finished int = 1
	largestTerm := currentTerm
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	// read-only varible is no need to lock
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				if reply.Term > largestTerm {
					largestTerm = reply.Term
				}
				if reply.VoteGranted {
					votes++
				}
				finished++
			}
			cond.Broadcast()
		}(i)
		Debug(dVote, "S%d T:%d -> S%d requestVote", rf.me, currentTerm, i)
	}
	mu.Lock()
	for finished < len(rf.peers) {
		cond.Wait()
		rf.mu.Lock()
		if largestTerm > rf.currentTerm {
			rf.currentTerm = largestTerm
			rf.votedFor = -1
			rf.role = Follower
			Debug(dInfo, "S%d T:%d becomes follower", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm == currentTerm && rf.role == Candidate && votes > len(rf.peers)/2 {
			rf.role = Leader
			Debug(dLeader, "S%d T:%d becomes leader", rf.me, rf.currentTerm)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
	mu.Unlock()
}

func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for !rf.killed() {
		for {
			needReplicate := func() bool {
				rf.mu.RLock()
				defer rf.mu.RUnlock()
				return rf.role == Leader && len(rf.log) > rf.matchIndex[server]+1
			}()
			if needReplicate {
				break
			}
			rf.replicatorCond[server].Wait()
		}
		rf.logReplication(server)
	}
}

func (rf *Raft) logReplication(server int) {
	rf.mu.RLock()
	currentTerm := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	entries := make([]Entry, 0)
	entries = append(entries, rf.log[prevLogIndex+1:]...)
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok {
		if reply.Success {
			if len(entries) > 0 {
				Debug(dLeader, "S%d T:%d -> S%d logReplication success: [%d, %d]", rf.me, currentTerm, server, prevLogIndex+1, prevLogIndex+len(entries))
			} else {
				Debug(dLeader, "S%d T:%d -> S%d heartBeat success", rf.me, currentTerm, server)
			}
			rf.mu.Lock()
			rf.matchIndex[server] = prevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			matchIndex := make([]int, len(rf.peers))
			copy(matchIndex, rf.matchIndex)
			sort.Sort(sort.Reverse(sort.IntSlice(matchIndex)))
			for i := len(rf.peers) / 2; i < len(matchIndex); i++ {
				if matchIndex[i] > rf.commitIndex && rf.log[matchIndex[i]].Term == rf.currentTerm {
					rf.commitIndex = matchIndex[i]
					Debug(dCommit, "S%d: SET commitIndex=%d", rf.me, rf.commitIndex)
					rf.applyCond.Signal()
					break
				}
			}
			rf.mu.Unlock()
		} else {
			if reply.Term > currentTerm {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = Follower
					Debug(dInfo, "S%d T%d: becomes follower", rf.me, rf.currentTerm)
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.nextIndex[server] = reply.NextIndex
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.role == Leader {
			rf.mu.RUnlock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.logReplication(i)
			}
			rf.mu.Lock()
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
		} else {
			rf.mu.RUnlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Entry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log[rf.lastApplied+1:rf.commitIndex+1])
		// Debug(dCommit, "S%d: ci [%d]", rf.me, rf.commitIndex+1)
		rf.mu.Unlock()
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
			Debug(dCommit, "S%d: log[%d]", rf.me, lastApplied+1+i)
		}
		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
}

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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.role = Follower
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{
		Command: nil,
		Term:    0,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicatorCond = make([]*sync.Cond, len(peers))
	for i := range peers {
		if i == me {
			continue
		}
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	Debug(dInfo, "S%d: Make", rf.me)

	return rf
}
