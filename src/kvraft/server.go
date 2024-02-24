package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key         string
	Value       string
	Type        OpType
	ClientId    int64
	SequenceNum int64
}

type OpReply struct {
	Err         Err
	Value       string
	SequenceNum int64
}

type Session struct {
	OpReplyCh              chan OpReply
	LastAppliedSequenceNum int64
	IsOnline               bool
	LastGetSequenceNum     int64
	LastGetValue           string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage           map[string]string
	sessions          map[int64]Session
	snapshotCond      *sync.Cond
	lastIncludedIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	currentTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dClient, "S%d T%d: C%d[%d]: GET %v", kv.me, currentTerm, args.ClientId, args.SequenceNum, args.Key)
	kv.mu.Lock()
	session, ok := kv.sessions[args.ClientId]
	if !ok {
		kv.sessions[args.ClientId] = Session{
			OpReplyCh:              make(chan OpReply),
			LastAppliedSequenceNum: 0,
			IsOnline:               false,
			LastGetSequenceNum:     0,
			LastGetValue:           "",
		}
		session = kv.sessions[args.ClientId]
	}
	if args.SequenceNum <= session.LastAppliedSequenceNum {
		if args.SequenceNum == session.LastGetSequenceNum {
			reply.Value = session.LastGetValue
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}
	session.IsOnline = true
	kv.sessions[args.ClientId] = session
	kv.mu.Unlock()
	command := Op{
		Key:         args.Key,
		Type:        OpGet,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	kv.rf.Start(command)
	opReply := OpReply{
		SequenceNum: -1,
	}
	for opReply.SequenceNum != args.SequenceNum {
		select {
		case opReply = <-session.OpReplyCh:
			reply.Value, reply.Err = opReply.Value, opReply.Err
		case <-time.After(100 * time.Millisecond):
			reply.Err = ErrTimeout
			opReply.SequenceNum = args.SequenceNum
		}
	}
	kv.mu.Lock()
	session = kv.sessions[args.ClientId]
	session.IsOnline = false
	kv.sessions[args.ClientId] = session
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	currentTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.Op == "Put" {
		Debug(dClient, "S%d T%d: C%d[%d]: PUT %v=%v", kv.me, currentTerm, args.ClientId, args.SequenceNum, args.Key, args.Value)
	} else {
		Debug(dClient, "S%d T%d: C%d[%d]: Append %v+=%v", kv.me, currentTerm, args.ClientId, args.SequenceNum, args.Key, args.Value)
	}
	kv.mu.Lock()
	session, ok := kv.sessions[args.ClientId]
	if !ok {
		kv.sessions[args.ClientId] = Session{
			OpReplyCh:              make(chan OpReply),
			LastAppliedSequenceNum: 0,
			IsOnline:               false,
			LastGetSequenceNum:     0,
			LastGetValue:           "",
		}
		session = kv.sessions[args.ClientId]
	}
	if args.SequenceNum <= session.LastAppliedSequenceNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	session.IsOnline = true
	kv.sessions[args.ClientId] = session
	kv.mu.Unlock()
	var opType OpType
	if args.Op == "Put" {
		opType = OpPut
	} else {
		opType = OpAppend
	}
	command := Op{
		Key:         args.Key,
		Value:       args.Value,
		Type:        opType,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	kv.rf.Start(command)
	opReply := OpReply{
		SequenceNum: -1,
	}
	for opReply.SequenceNum != args.SequenceNum {
		select {
		case opReply = <-session.OpReplyCh:
			reply.Err = opReply.Err
		case <-time.After(100 * time.Millisecond):
			reply.Err = ErrTimeout
			opReply.SequenceNum = args.SequenceNum
		}
	}
	kv.mu.Lock()
	session = kv.sessions[args.ClientId]
	session.IsOnline = false
	kv.sessions[args.ClientId] = session
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			kv.snapshotCond.Signal()
			kv.mu.Lock()
			session, ok := kv.sessions[op.ClientId]
			if !ok {
				kv.sessions[op.ClientId] = Session{
					OpReplyCh:              make(chan OpReply),
					LastAppliedSequenceNum: 0,
					IsOnline:               false,
					LastGetSequenceNum:     0,
					LastGetValue:           "",
				}
				session = kv.sessions[op.ClientId]
			}
			if op.SequenceNum > session.LastAppliedSequenceNum {
				if op.Type == OpGet {
					value, hasKey := kv.storage[op.Key]
					if hasKey {
						session.LastGetSequenceNum = op.SequenceNum
						session.LastGetValue = value
					}
				} else if op.Type == OpAppend {
					value, hasKey := kv.storage[op.Key]
					if hasKey {
						kv.storage[op.Key] = value + op.Value
					} else {
						kv.storage[op.Key] = op.Value
					}
				} else {
					kv.storage[op.Key] = op.Value
				}
			}
			session.LastAppliedSequenceNum = op.SequenceNum
			if session.IsOnline {
				value := session.LastGetValue
				err := OK
				if op.Type == OpGet && session.LastGetSequenceNum != op.SequenceNum {
					err = ErrNoKey
				}
				go func(value string, err Err, sequenceNum int64, opReplyCh chan OpReply) {
					opReplyCh <- OpReply{
						Value:       value,
						Err:         err,
						SequenceNum: sequenceNum,
					}
				}(value, Err(err), op.SequenceNum, session.OpReplyCh)
			}
			kv.sessions[op.ClientId] = session
			if log.CommandIndex > kv.lastIncludedIndex {
				kv.lastIncludedIndex = log.CommandIndex
			}
			kv.mu.Unlock()
		} else if log.SnapshotValid {
			kv.mu.Lock()
			r := bytes.NewBuffer(kv.persister.ReadSnapshot())
			d := labgob.NewDecoder(r)
			var storage map[string]string
			var sessions map[int64]Session
			if d.Decode(&storage) == nil {
				kv.storage = storage
			}
			if d.Decode(&sessions) == nil {
				kv.sessions = sessions
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) snapshoter() {
	kv.snapshotCond.L.Lock()
	defer kv.snapshotCond.L.Unlock()
	for !kv.killed() {
		for kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate/10*9 {
			kv.snapshotCond.Wait()
		}
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.storage)
		e.Encode(kv.sessions)
		kv.rf.Snapshot(kv.lastIncludedIndex, w.Bytes())
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = make(map[string]string)
	kv.sessions = make(map[int64]Session)
	if persister.ReadSnapshot() != nil || len(persister.ReadSnapshot()) > 0 {
		r := bytes.NewBuffer(persister.ReadSnapshot())
		d := labgob.NewDecoder(r)
		var storage map[string]string
		var sessions map[int64]Session
		if d.Decode(&storage) == nil {
			kv.storage = storage
		}
		if d.Decode(&sessions) == nil {
			kv.sessions = sessions
		}
	}
	kv.snapshotCond = sync.NewCond(&sync.Mutex{})

	go kv.applier()
	go kv.snapshoter()

	return kv
}
