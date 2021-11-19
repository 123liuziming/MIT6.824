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
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"

import "6.824/labgob"



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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const TIMEOUTMAX = 500
const TIMEOUTMIN = 200

func min(a int32, b int32) int32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.raftState.currentTerm)
	e.Encode(rf.raftState.voteFor)
	e.Encode(rf.raftState.logs)
	return w.Bytes()
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState   State
	leaderIndex int
	voteNum     int32
	message     chan ApplyMsg
	// 几个channel，定时器中使用
	appendEntryChan chan bool
	voteForChan     chan bool
	//aeLocks []sync.Mutex
}

type Log struct {
	Index   int32
	Term    int32
	Command interface{}
}

type State struct {
	//持久存储的状态
	currentTerm int32
	voteFor     int
	logs        []Log
	//非持久化的
	commitIndex int32
	lastApplied int
	nextIndex   []int32
	matchIndex  []int32
	logOffset   int32
	logIndex    int32
}

var broadcast int32 = 0

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.raftState.currentTerm), rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// 只在服务器回复一个RPC或者发送一个RPC时，服务器才进行持久化存储，这样可以节省一些持久化存储的操作
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	// 需要保存的状态有：currentTerm, voteFor, log[]
	encoder.Encode(rf.raftState.currentTerm)
	encoder.Encode(rf.raftState.voteFor)
	encoder.Encode(rf.raftState.logs)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var voteFor int
	var currentTerm int32
	var logs []Log
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&voteFor) != nil || decoder.Decode(&logs) != nil {
		log.Fatal("decode fail!!!")
	} else {
		rf.raftState.currentTerm, rf.raftState.voteFor, rf.raftState.logs = currentTerm, voteFor, logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	offset := int(rf.getFirstLog().Index)
	if lastIncludedIndex <= offset {
		return false
	}
	rf.deleteLogsTo(lastIncludedIndex - offset)
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	rf.raftState.logs[0].Term, rf.raftState.logs[0].Index, rf.raftState.lastApplied, rf.raftState.commitIndex = int32(lastIncludedTerm), int32(lastIncludedIndex), lastIncludedIndex, max(rf.raftState.commitIndex, int32(lastIncludedIndex))
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	offset := int(rf.getFirstLog().Index)
	if index <= offset {
		return
	}
	rf.deleteLogsTo(index - offset)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	reply.Term = rf.raftState.currentTerm

	if arg.Term < reply.Term {
		rf.mu.Unlock()
		return
	}
	rf.updateTerm(arg.Term)
	send(rf.appendEntryChan)
	if !rf.CondInstallSnapshot(arg.LastIncludedTerm, arg.LastIncludedIndex, arg.Data) {
		rf.mu.Unlock()
		return
	}
	rf.message <- ApplyMsg{CommandValid: false, Snapshot: arg.Data}
	rf.mu.Unlock()
}

func (rf *Raft) sendSnapshot(i int) {
	rf.mu.Lock()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapShotArgs{Term: rf.raftState.currentTerm, LeaderId: rf.me, LastIncludedIndex: int(rf.getFirstLog().Index), LastIncludedTerm: int(rf.getFirstLog().Term), Data: rf.persister.ReadSnapshot()}
	reply := InstallSnapShotReply{}
	rf.mu.Unlock()
	ok := rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.raftState.currentTerm {
		rf.updateTerm(reply.Term)
		return
	}
	rf.raftState.matchIndex[i] = rf.getFirstLog().Index

	rf.raftState.nextIndex[i] = rf.raftState.matchIndex[i] + 1
	N := rf.raftState.matchIndex[i]

	if rf.isLeader() && rf.moreThanHalf(N) && N > rf.raftState.commitIndex && rf.getLogEntry(N).Term == rf.raftState.currentTerm {
		rf.updateCommitIndex(N)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []Log
	LeaderCommit int32
}

type FastRecoveryEntry struct {
	// term of conflicting entry
	XTerm int32
	// index of first entry if XTerm
	XIndex int32
	// num of empty slot
	XLen int32
	// 此条目是否有效(无效说明已经同步成功了)
	Done bool
	// 是否是超时条目
	TimeOut bool
}

type AppendEntriesReply struct {
	Term         int32
	Success      bool
	FastRecovery FastRecoveryEntry
}

type InstallSnapShotArgs struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapShotReply struct {
	Term int32
}

func send(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.raftState.currentTerm {
		rf.updateTerm(args.Term)
	}

	replyEntity := RequestVoteReply{Term: rf.raftState.currentTerm}
	if args.Term < rf.raftState.currentTerm {
		*reply = replyEntity
		return
	}

	isUpToDate := func() bool {
		return args.LastLogTerm > rf.raftState.logs[len(rf.raftState.logs)-1].Term || (args.LastLogTerm == rf.raftState.logs[len(rf.raftState.logs)-1].Term && args.LastLogIndex >= rf.raftState.logs[len(rf.raftState.logs)-1].Index)
	}
	if (rf.raftState.voteFor == -1 || rf.raftState.voteFor == args.CandidateId) && isUpToDate() {
		rf.raftState.voteFor, replyEntity.VoteGranted = args.CandidateId, true
		send(rf.voteForChan)
	}

	*reply = replyEntity
}

// 二分寻找logs数组中term为某XTerm的第一条Log条目的index
func (rf *Raft) binarySearchFirstXIndex(xTerm int32) int32 {

	l, r := int32(0), int32(len(rf.raftState.logs))-1

	for l < r {
		mid := l + ((r - l) >> 1)
		if rf.raftState.logs[mid].Term >= xTerm {
			r = mid
		} else if rf.raftState.logs[mid].Term < xTerm {
			l = mid + 1
		}
	}

	if rf.raftState.logs[l].Term == xTerm {
		return rf.raftState.logs[l].Index
	}

	return -1
}

// 二分寻找logs数组中term为某XTerm的最后一条Log条目的index
func (rf *Raft) binarySearchLastXIndex(xTerm int32) int32 {
	l, r := int32(0), int32(len(rf.raftState.logs))-1

	for l <= r {
		mid := l + ((r - l) >> 1)
		if rf.raftState.logs[mid].Term > xTerm {
			r = mid - 1
		} else if rf.raftState.logs[mid].Term <= xTerm {
			l = mid + 1
		}
	}

	if r >= 0 && rf.raftState.logs[r].Term == xTerm {
		return rf.raftState.logs[r].Index
	}

	return -1
}

// Append Entries RPC handler
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	send(rf.appendEntryChan)
	replyEntity := AppendEntriesReply{Term: rf.raftState.currentTerm}
	currentTerm := rf.raftState.currentTerm
	rf.updateTerm(args.Term)

	// 收到了过期的AE
	if args.Term < currentTerm {
		replyEntity.FastRecovery.TimeOut = true
		*reply = replyEntity
		return
	}

	rf.leaderIndex = args.LeaderId

	// prevLogIndex上的日志条目不匹配
	// 1.follower日志太短了，在冲突的位置没有Log条目
	if args.PrevLogIndex > rf.getLastLog().Index {
		replyEntity.FastRecovery.XTerm, replyEntity.FastRecovery.XLen = -1, args.PrevLogIndex-rf.getLastLog().Index
		*reply = replyEntity
		return
	}

	replyEntity.FastRecovery.Done = true
	// 如果冲突的条目，删掉之后的所有日志
	if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		xTerm := rf.getLogEntry(args.PrevLogIndex).Term
		xIndex := rf.binarySearchFirstXIndex(xTerm)
		replyEntity.FastRecovery.XTerm, replyEntity.FastRecovery.XIndex, replyEntity.FastRecovery.Done = xTerm, xIndex, false
		rf.deleteLogsFrom(args.PrevLogIndex)
		replyEntity.Success = true
		*reply = replyEntity
		return
	}

	if args.Entries != nil {
		index := args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			index++
			if index <= rf.getLastLog().Index {
				if rf.getLogEntry(index).Term == args.Entries[i].Term {
					continue
				} else {
					rf.deleteLogsFrom(index)
				}
			}
			rf.appendLogs(args.Entries[i:])
			break
		}
	}

	if args.LeaderCommit > rf.raftState.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, rf.getLastLog().Index))
	}

	replyEntity.Success = true
	*reply = replyEntity
}

func (rf *Raft) isLeader() bool {
	return rf.leaderIndex == rf.me
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

var ErrTimeout = errors.New("timeout")

func RunWithTimeout(handler func(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool, timeout time.Duration, i int, args *AppendEntriesArgs, reply *AppendEntriesReply) (error, bool) {
	done := make(chan bool, 1)
	go func() {
		handler(i, args, reply)
		done <- true
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ErrTimeout, false
	case <-done:
		return nil, true
	}
}

func (rf *Raft) sendAppendEntries() {
	syncLog := func(i int) {
		rf.mu.Lock()
		var prevLogIndex = rf.raftState.nextIndex[i] - 1
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			reply := AppendEntriesReply{}
			if !rf.isLeader() {
				rf.mu.Unlock()
				return
			}
			if prevLogIndex > rf.getLastLog().Index || prevLogIndex < rf.getFirstLog().Index {
				if prevLogIndex < rf.getFirstLog().Index {
					go rf.sendSnapshot(i)
				}
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{Term: rf.raftState.currentTerm, LeaderId: rf.leaderIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: rf.getLogEntry(prevLogIndex).Term, Entries: append(make([]Log, 0), rf.raftState.logs[prevLogIndex-rf.getFirstLog().Index+1:]...), LeaderCommit: rf.raftState.commitIndex}
			rf.mu.Unlock()
			ok := rf.sendAppendEntry(i, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			rf.updateTerm(reply.Term)
			if !rf.isLeader() {
				rf.mu.Unlock()
				return
			}

			bs := rf.binarySearchLastXIndex(reply.FastRecovery.XTerm)
			if reply.FastRecovery.TimeOut {
				rf.mu.Unlock()
				continue
			}
			if reply.Success && reply.FastRecovery.Done {
				// 更新follower的nextIndex与matchIndex
				rf.raftState.matchIndex[i] = args.PrevLogIndex + int32(len(args.Entries))
				rf.raftState.nextIndex[i] = rf.raftState.matchIndex[i] + 1

				N := rf.raftState.matchIndex[i]
				if rf.isLeader() && rf.moreThanHalf(N) && N > rf.raftState.commitIndex && rf.getLogEntry(N).Term == rf.raftState.currentTerm {
					rf.updateCommitIndex(N)
				}
				rf.mu.Unlock()
				break
			}
			if !reply.Success && reply.FastRecovery.XTerm == -1 {
				prevLogIndex -= reply.FastRecovery.XLen
			} else if bs == -1 {
				// Leader中没有任期为XTerm的日志
				prevLogIndex = reply.FastRecovery.XIndex
			} else {
				prevLogIndex = bs + 1
			}
			rf.mu.Unlock()
		}
	}
	for i, _ := range rf.raftState.nextIndex {
		if i != rf.me && rf.isLeader() {
			go syncLog(i)
		}
	}
}

func (rf *Raft) sendAppendEntry(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.AppendEntry", args, reply)
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
	defer rf.mu.Unlock()
	index, term, isLeader := -1, -1, rf.isLeader()
	if !isLeader {
		return index, term, isLeader
	}
	//将log添加到本地
	logs := []Log{{Term: rf.raftState.currentTerm, Command: command}}
	lastLogIndex := rf.appendLogs(logs)
	//rf.sendAppendEntries()
	//wg := sync.WaitGroup{}
	return int(lastLogIndex), int(rf.raftState.currentTerm), rf.isLeader()
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

func (rf *Raft) increaseTerm() {
	atomic.AddInt32(&rf.raftState.currentTerm, 1)
}

func (rf *Raft) updateTerm(newTerm int32) {
	defer rf.persist()

	if newTerm > rf.raftState.currentTerm {
		rf.leaderIndex, rf.raftState.voteFor = -1, -1
		atomic.CompareAndSwapInt32(&rf.raftState.currentTerm, rf.raftState.currentTerm, newTerm)
	}
}

func (rf *Raft) increaseVoteNum() {
	atomic.CompareAndSwapInt32(&rf.voteNum, rf.voteNum, rf.voteNum+1)
}

func (rf *Raft) appendLogs(logs []Log) int32 {
	for i := range logs {
		logs[i].Index = rf.raftState.logIndex
		rf.raftState.logIndex++
	}
	rf.raftState.logs = append(rf.raftState.logs, logs...)
	rf.persist()
	return rf.raftState.logIndex - 1
}

func (rf *Raft) logsLen() int32 {
	return int32(len(rf.raftState.logs))
}

func (rf *Raft) deleteLogsTo(index int) {
	if index >= len(rf.raftState.logs) {
		rf.raftState.logIndex = rf.raftState.logs[0].Index + int32(index) + 1
		rf.raftState.logs = make([]Log, 1)
		rf.raftState.logs[0].Command = nil
	} else {
		if index >= 0 {
			for i := 0; i < index; i++ {
				rf.raftState.logs[i].Command = nil
			}
			rf.raftState.logs = append(make([]Log, 0), rf.raftState.logs[index:]...)
			rf.raftState.logs[0].Command = nil
		}
	}
	rf.persist()
}

func (rf *Raft) deleteLogsFrom(index int32) {
	defer rf.persist()
	index -= rf.raftState.logs[0].Index
	if index < int32(len(rf.raftState.logs)) && index >= 0 {
		rf.raftState.logIndex = rf.raftState.logs[index].Index
		rf.raftState.logs = rf.raftState.logs[:index]
	}
}

func (rf *Raft) getLogEntry(index int32) Log {
	index = index - rf.raftState.logs[0].Index
	if index >= 0 {
		return rf.raftState.logs[index]
	} else {
		return Log{Term: -1}
	}
}

func (rf *Raft) getLastLog() Log {
	if len(rf.raftState.logs) > 0 {
		return rf.raftState.logs[len(rf.raftState.logs)-1]
	} else {
		return Log{0, -1, ""}
	}
}

func (rf *Raft) getFirstLog() Log {
	if len(rf.raftState.logs) > 0 {
		return rf.raftState.logs[0]
	} else {
		return Log{0, -1, ""}
	}
}

func (rf *Raft) updateCommitIndex(newCommitIndex int32) {
	maxIndex := max(rf.raftState.commitIndex+1, 1)
	logsCopy := make([]Log, len(rf.raftState.logs))
	copy(logsCopy, rf.raftState.logs)
	offset := logsCopy[0].Index
	go func() {
		for i := max(maxIndex, offset+1); i <= newCommitIndex; i++ {
			rf.message <- ApplyMsg{Command: logsCopy[i-offset].Command, CommandValid: true, CommandIndex: int(i)}
		}
	}()
	rf.raftState.commitIndex = max(rf.raftState.commitIndex, newCommitIndex)
}

func (rf *Raft) reInitializeIndex() {
	rf.raftState.matchIndex = make([]int32, len(rf.peers))
	rf.raftState.nextIndex = make([]int32, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.raftState.nextIndex[i] = rf.raftState.logs[len(rf.raftState.logs)-1].Index + 1
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.increaseTerm()
	rf.voteNum, rf.raftState.voteFor = 1, rf.me
	rf.persist()
	//是否需要发送append entries广播
	atomic.StoreInt32(&broadcast, 0)
	rf.mu.Unlock()
	sendVoteFun := func(i int) {
		rf.mu.Lock()
		lastLog := rf.getLastLog()
		voteArgs := RequestVoteArgs{Term: rf.raftState.currentTerm, CandidateId: rf.me, LastLogIndex: lastLog.Index, LastLogTerm: lastLog.Term}
		voteReply := RequestVoteReply{}
		rf.mu.Unlock()
		ok := rf.sendRequestVote(i, &voteArgs, &voteReply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		rf.updateTerm(voteReply.Term)
		if atomic.LoadInt32(&broadcast) == 0 && voteReply.VoteGranted {
			rf.increaseVoteNum()
			if int(atomic.LoadInt32(&rf.voteNum)) > len(rf.peers)/2 {
				rf.leaderIndex = rf.me
				atomic.StoreInt32(&broadcast, 1)
				rf.persist()
				// 如果已经是leader，发送AppendEntries RPC
				if atomic.LoadInt32(&broadcast) == 1 {
					// 重新初始化nextIndex与matchIndex
					rf.reInitializeIndex()
					rf.sendAppendEntries()
					send(rf.voteForChan)
				}
			}
		}
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go sendVoteFun(i)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.isLeader()
		rf.mu.Unlock()
		if isLeader {
			rf.mu.Lock()
			rf.sendAppendEntries()
			rf.mu.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
		} else {
			rd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me*1000)))
			random := time.Duration(rd.Intn(TIMEOUTMAX-TIMEOUTMIN)+TIMEOUTMIN) * time.Millisecond
			select {
			case <-rf.voteForChan:
			case <-rf.appendEntryChan:
			case <-time.After(random):
				// 超时，开始选举
				rf.election()
			}
		}
	}
}

func (rf *Raft) moreThanHalf(n int32) bool {
	if n < 1 || n > rf.getLastLog().Index {
		return false
	}
	count := 1
	for i, val := range rf.raftState.matchIndex {
		if i != rf.me && val >= n {
			count++
		}
	}
	return count > len(rf.peers)/2
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.leaderIndex, rf.raftState.voteFor, rf.raftState.commitIndex, rf.message = -1, -1, -1, applyCh
	rf.raftState.logs = make([]Log, 1)
	rf.raftState.logs[0] = Log{Term: -1}
	rf.reInitializeIndex()
	rf.voteForChan = make(chan bool, 100)
	rf.appendEntryChan = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.raftState.logIndex = rf.getLastLog().Index + 1
	//go func() {
	rf.message <- ApplyMsg{CommandValid: false, Snapshot: rf.persister.ReadSnapshot()}
	//}()
	rf.persist()
	//rf.aeLocks = make([]sync.Mutex, len(rf.peers))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
