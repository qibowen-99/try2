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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the apply passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the apply; at that point you can add fields to
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's isLeader
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted isLeader
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// isLeader a Raft server must maintain.
	curTerm int 			
	voteFor int        			
	Log []Log
	comIndex int 
	lastApplied int

	electionTimeout	int			
	heartbeatPeriod	int			
	latestIssueTime	int64	
	latestHeardTime	int64

	// leaders
	nextIndex	[]int			
	matchIndex	[]int
	numVotes	int
	isLeader 	int	// 0: follower; 1: candidate; 2: leader		
	leaderId	int				
	applyCond	*sync.Cond		
	leaderCond	*sync.Cond		
	nonLeaderCond 	*sync.Cond	

	// channels
	apply		chan ApplyMsg	
	receiveHeartBeats	chan bool	
	electionTimeouts	chan bool
	grantedVote chan bool
	allReplyReceived chan bool
	newEntrayStart chan bool
	newcommit chan bool			
}

type Log struct{
	Index int
	Command interface{}
	Term int
}

// return curTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.curTerm

	if rf.isLeader==2 {
		isleader=true
	}else{
		isleader=false
	}
	return term, isleader
}

func (rf *Raft) IsLeader()(bool){
	return rf.isLeader==2
}

//
// save Raft's persistent isLeader to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
// restore previously persisted isLeader.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any isLeader?
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastIndex int
	LastTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  bool
}

type AppendEntriesArgs struct {
	Term 			int			
	LeaderId		int					
	Entries			[]Log	
	LeaderCommit	int	
	PrevLogIndex	int			
	PrevLogTerm		int			
}

type AppendEntriesReply struct {
	Term 			int	
	Success			bool
	IndexExpect	 int		
	ConflictTerm	int			
	ConflictFirstIndex	int				
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.curTerm
	reply.VoteGranted = false

	// Early return if term is less than current term
	if rf.curTerm > args.Term {
		return
	}

	// If term is larger, update term and become follower
	if rf.curTerm < args.Term {
		rf.curTerm = args.Term
		rf.voteFor = -1
		rf.jump(0)
		rf.persist()
	}

	// If not voted or voted for the candidate, check log up-to-dateness
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		return
	}

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.Log[lastLogIndex].Term
	}

	if lastLogTerm > args.LastTerm || (lastLogTerm == args.LastTerm && lastLogIndex > args.LastIndex) {
		return
	}

	// Candidate's log is at least as up-to-date as receiver's log, grant vote
	rf.voteFor = args.CandidateId
	rf.resetElectionTimer()
	rf.jump(0)
	rf.persist()

	reply.VoteGranted = true
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
	
	if !ok || rf.isLeader != 1 || args.Term != rf.curTerm {
		return ok
	}

	if !reply.VoteGranted {
		rf.curTerm = reply.Term
		rf.isLeader = 0
		return ok
	}

	rf.numVotes++
	if rf.numVotes > len(rf.peers)/2 {
		rf.isLeader = 2
		rf.allReplyReceived <- true
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.curTerm
	reply.Success = false

	// Current term is less than the incoming term
	if rf.curTerm <= args.Term {
		rf.curTerm = args.Term
		rf.voteFor = -1
		rf.jump(0)
		rf.resetElectionTimer()

		// Check if log contains an entry at PrevLogIndex with term PrevLogTerm
		if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.jump(0)

			// Check if entries match from PrevLogIndex
			if !isEntriesMatched(rf.Log, args.Entries, args.PrevLogIndex) {
				// Copy and replace entries from leader
				entries := make([]Log, len(args.Entries))
				copy(entries, args.Entries)
				rf.Log = append(rf.Log[:args.PrevLogIndex+1], entries...)
			}

			// Update commit index
			newEntriesLastIndex := args.PrevLogIndex + len(args.Entries)
			rf.updateCommitIndex(args.LeaderCommit, newEntriesLastIndex)
			
			rf.leaderId = args.LeaderId
			rf.resetElectionTimer()
			reply.Term = rf.curTerm
			reply.Success = true

			return
		}

		// Handle log inconsistency
		conflictIndex, conflictTerm := rf.getConflictInfo(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm
		reply.ConflictFirstIndex = conflictIndex
	}

	rf.persist()
}

// Check if entries match from specific log index
func isEntriesMatched(log []Log, entries []Log, prevLogIndex int) bool {
	isMatched := true
	nextIndex := prevLogIndex + 1
	end := len(log) - 1
	for i := 0; isMatched && i < len(entries); i++ {
		if end < nextIndex+i || log[nextIndex+i].Term != entries[i].Term {
			isMatched = false
		}
	}

	return isMatched
}

// Get conflict info
func (rf *Raft) getConflictInfo(prevLogIndex int) (int, int) {
	conflictIndex := prevLogIndex + 1
	conflictTerm := 0
	if len(rf.Log) < conflictIndex {
		conflictIndex = len(rf.Log)
	} else {
		conflictTerm = rf.Log[prevLogIndex].Term
		for i := conflictIndex - 1; i >= 0; i-- {
			if rf.Log[i].Term != conflictTerm {
				break
			} else {
				conflictIndex--
			}
		}
	}

	return conflictIndex, conflictTerm
}

// Update commit index
func (rf *Raft) updateCommitIndex(leaderCommit, newEntriesLastIndex int) {
	if leaderCommit > rf.comIndex {
		if leaderCommit < newEntriesLastIndex {
			rf.comIndex = leaderCommit
		} else {
			rf.comIndex = newEntriesLastIndex
		}
		rf.applyCond.Broadcast()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkAndSendAppendEntries() {
    lastlogindex := rf.Log[len(rf.Log)-1].Index
    halfPeerSize := len(rf.peers) / 2
    for i := rf.comIndex + 1; i <= lastlogindex; i++ {
        totalcommits := 1
        for j := range rf.peers {
            if j != rf.me && rf.matchIndex[j] >= i {
                totalcommits++
            }
        }
        if totalcommits > halfPeerSize {
            rf.comIndex = i
            rf.newcommit <- true
        }
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {

		rf.mu.Lock()
		logEntry := Log{Command:command, Term:rf.curTerm}
		rf.Log = append(rf.Log, logEntry)
		index = len(rf.Log) - 1
		numReplica := 1
		rf.latestIssueTime = time.Now().UnixNano()

		rf.persist()
		rf.mu.Unlock()
		rf.mu.Lock()
		go rf.broadcastAppendEntries(index, rf.curTerm, rf.comIndex, numReplica, "Start")
		rf.mu.Unlock()

	}
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

func (raft *Raft) updateNextIndex(reply AppendEntriesReply, currentIndex int) int {

    if reply.ConflictTerm == 0 {
        // If there's no conflict, set currentIndex to ConflictFirstIndex.
        currentIndex = reply.ConflictFirstIndex
    } else {

        searchIndex := reply.ConflictFirstIndex
        logTermAtConflict := raft.Log[searchIndex].Term

        if logTermAtConflict >= reply.ConflictTerm {
            // Search backwards to find the first entry with the same term.
            searchIndex = raft.backwardSearch(reply, searchIndex)
            
            // If an entry with the same term is found, update currentIndex.
            if searchIndex != 0 {
                searchIndex = raft.forwardSearch(reply, currentIndex, searchIndex)
                currentIndex = searchIndex + 1
            } else {
                // If no entry with the same term is found, set currentIndex to ConflictFirstIndex.
                currentIndex = reply.ConflictFirstIndex
            }
        } else {
            // If term is less than ConflictTerm, the search is futile.
            currentIndex = reply.ConflictFirstIndex
        }
    }

    return currentIndex
}

func (raft *Raft) backwardSearch(reply AppendEntriesReply, searchIndex int) int {
    // Backward search for an entry with the same term.
    for i := searchIndex; i > 0; i-- {
        if raft.Log[i].Term == reply.ConflictTerm {
            break
        }
        searchIndex -= 1
    }
    return searchIndex
}

func (raft *Raft) forwardSearch(reply AppendEntriesReply, currentIndex int, searchIndex int) int {
    // Forward search to find the last entry with the same term.
    for i := searchIndex + 1; i < currentIndex; i++ {
        if raft.Log[i].Term != reply.ConflictTerm {
            break
        }
        searchIndex += 1
    }
    return searchIndex
}


const MAX_RETRY_COUNT = 3

func (rf *Raft) broadcastAppendEntries(index int, term int, comIndex int, nReplica int, name string) {
	var wg sync.WaitGroup
	majority := len(rf.peers)/2 + 1
	isAgree := false

	if _, isLeader := rf.GetState(); !isLeader || rf.curTerm != term {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go func(i int, rf *Raft) {
			defer wg.Done()

			retryCount := 0
			for {
				if _, isLeader := rf.GetState(); !isLeader {
					return
				}

				rf.mu.Lock()
				if rf.curTerm != term {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[i]
				prevLogIndex := nextIndex - 1
				prevLogTerm := rf.Log[max(prevLogIndex,0)].Term
				entries := make([]Log, 0)
				if nextIndex < index+1 {
					entries = rf.Log[nextIndex : index+1]
				}

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: comIndex,
				}
				rf.mu.Unlock()
				var reply AppendEntriesReply

				ok := rf.sendAppendEntries(i, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				if rf.curTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				if reply.Success == false {
					if args.Term < reply.Term {
						rf.curTerm = reply.Term
						rf.voteFor = -1
						rf.jump(0)

						rf.persist()

						rf.mu.Unlock()
						return
					} else {
						nextIndex := rf.updateNextIndex(reply, nextIndex)
						rf.nextIndex[i] = nextIndex
						rf.mu.Unlock()
						if retryCount > MAX_RETRY_COUNT {
							return
						}
						retryCount++
						continue
					}
				} else {
					if rf.nextIndex[i] < index+1 {
						rf.nextIndex[i] = index + 1
						rf.matchIndex[i] = index
					}
					nReplica += 1
					if !isAgree && rf.isLeader == 2 && nReplica >= majority {
						isAgree = true
						if rf.comIndex < index && rf.Log[index].Term == rf.curTerm {
							rf.comIndex = index
							go rf.broadcastHeartbeat()

							rf.applyCond.Broadcast()

						}
					}
					rf.mu.Unlock()
					return
				}
			}
		}(i, rf)
	}

	wg.Wait()
}

func (rf *Raft) applyEntries() {
	for {
		rf.mu.Lock()

		// The command is already applied, wait for a new one
		if rf.lastApplied == rf.comIndex {
			rf.applyCond.Wait()
			rf.mu.Unlock()
			continue
		}

		// Apply all entries that haven't been applied yet
		for i := rf.lastApplied + 1; i <= rf.comIndex; i++ {
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.Log[i].Command, CommandIndex: i}
			rf.lastApplied = i
			rf.mu.Unlock() // Unlock before sending to avoid deadlock
			rf.apply <- applyMsg
			rf.mu.Lock() // Lock again for the next iteration
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) jump(newState int) {
	oldState := rf.isLeader
	rf.isLeader = newState
	if oldState == 2 && newState == 0 {
		rf.nonLeaderCond.Broadcast()
	} else if oldState == 1 && newState == 2 {
		rf.leaderCond.Broadcast()
	}
}


func (rf *Raft) runElectionTimeoutChecker() {
	for {
		if term, isLeader := rf.GetState(); isLeader {
			rf.awaitNonLeaderCondition()
		} else {
			rf.checkElectionTimeout(term)
			time.Sleep(time.Millisecond*10)  // Maintain the sleep time for the checker.
		}
	}
}

func (rf *Raft) awaitNonLeaderCondition() {
	rf.mu.Lock()
	rf.nonLeaderCond.Wait()
	rf.mu.Unlock()
}

func (rf *Raft) checkElectionTimeout(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	elapseTime := time.Now().UnixNano() - rf.latestHeardTime
	if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout {
		rf.electionTimeouts <- true
	}
}


func (rf *Raft) runHeartbeatPeriodChecker() {
	for {
		if term, isLeader := rf.GetState(); !isLeader {
			rf.awaitLeaderCondition()
		} else {
			rf.checkHeartbeatPeriod(term)
			time.Sleep(time.Millisecond*10)  // Maintain the sleep time for the checker.
		}
	}
}

func (rf *Raft) awaitLeaderCondition() {
	rf.mu.Lock()
	rf.leaderCond.Wait()
	rf.mu.Unlock()
}

func (rf *Raft) checkHeartbeatPeriod(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	elapseTime := time.Now().UnixNano() - rf.latestIssueTime
	if int(elapseTime/int64(time.Millisecond)) >= rf.heartbeatPeriod {
		rf.receiveHeartBeats <- true
	}
}


func (rf *Raft) eventLoop() {
	for {
		select {
		case <- rf.electionTimeouts:
			go rf.startElection()
		case <- rf.receiveHeartBeats:
			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	rf.latestIssueTime = time.Now().UnixNano()
	index := len(rf.Log) - 1
	nReplica := 1
	go rf.broadcastAppendEntries(index, rf.curTerm, rf.comIndex, nReplica, "Broadcast")
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = rf.heartbeatPeriod*5 + rand.Intn(200)
	rf.latestHeardTime = time.Now().UnixNano()
}


func (rf *Raft) startElection() {
	rf.resetAndPersistState()

	numVotes := 1
	var wg sync.WaitGroup
	winThreshold := rf.quorumSize()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go rf.sendVoteRequest(i, &numVotes, &wg, winThreshold)
	}
	wg.Wait()
}

func (rf *Raft) resetAndPersistState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.jump(1)
	rf.curTerm += 1
	rf.voteFor = rf.me
	rf.resetElectionTimer()
	rf.persist()
}

func (rf *Raft) sendVoteRequest(i int, numVotes *int, wg *sync.WaitGroup, winThreshold int) {
	defer wg.Done()

	rf.mu.Lock()
	lastLogIndex := len(rf.Log) - 1
	args := RequestVoteArgs{
		Term:        rf.curTerm,
		CandidateId: rf.me,
		LastIndex:   lastLogIndex,
		LastTerm:    rf.Log[lastLogIndex].Term,
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	ok := rf.sendRequestVote(i, &args, &reply)

	if ok {
		rf.handleRequestVoteReply(&args, &reply, numVotes, winThreshold)
	}
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply, numVotes *int, winThreshold int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm != args.Term {
		return
	}

	if !reply.VoteGranted {
		if rf.curTerm < reply.Term {
			rf.curTerm = reply.Term
			rf.voteFor = -1
			rf.jump(0)
			rf.persist()
		}
	} else {
		*numVotes += 1
		if rf.isLeader == 1 && *numVotes >= winThreshold {
			rf.jump(2)
			rf.leaderId = rf.me
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.Log)
				rf.matchIndex[i] = 0
			}
			go rf.broadcastHeartbeat()
			rf.persist()
		}
	}
}

func (rf *Raft) quorumSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.peers)/2 + 1
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent isLeader, and also initially holds the most
// recent saved isLeader, if any. apply is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, apply chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		apply:             apply,
		isLeader:          0,
		leaderId:          -1,
		curTerm:           0,
		voteFor:           -1,
		comIndex:          0,
		lastApplied:       0,
		heartbeatPeriod:   100,
		electionTimeouts:  make(chan bool),
		receiveHeartBeats: make(chan bool),
		Log:               append([]Log{{Term: 0}}),
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}

	// Initialize synchronization primitives after struct creation
	rf.resetElectionTimer()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.nonLeaderCond = sync.NewCond(&rf.mu)

	// Start goroutines
	go rf.runElectionTimeoutChecker()
	go rf.runHeartbeatPeriodChecker()
	go rf.eventLoop()
	go rf.applyEntries()

	// Load persisted state
	rf.mu.Lock()
	rf.readPersist(rf.persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}
