package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpName string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your definitions here.
	maxraftstate              int               // snapshot if log grows this big
	kvStore                   map[string]string // key-value store
	logIndexVsOpSucceededChan map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OpName: "Get", Key: args.Key}
	ok := kv.getConsensusFromRAFT(op)

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	kv.mu.Lock()
	val, isKeyPresent := kv.kvStore[args.Key]
	kv.mu.Unlock()

	if isKeyPresent {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpName: args.Op, Key: args.Key, Value: args.Value}
	ok := kv.getConsensusFromRAFT(op)

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (kv *KVServer) getConsensusFromRAFT(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		fmt.Printf("[SERVER] [%d] NOT LEADER \n", kv.me)
		return false
	}
	fmt.Printf("[SERVER] [%d] raft returned index = %d\n", kv.me, index)

	kv.mu.Lock()
	ch, ok := kv.logIndexVsOpSucceededChan[index]

	if !ok {
		ch = make(chan Op, 1)
		kv.logIndexVsOpSucceededChan[index] = ch
	}

	kv.mu.Unlock()

	fmt.Printf("[SERVER] [%d] waiting for index = %d\n", kv.me, index)
	select {
	case cmd := <-ch:
		fmt.Printf("[SERVER] [%d] got index = %d, match? %t\n", kv.me, index, cmd == op)
		return cmd == op
	case <-time.After(800 * time.Millisecond):
		fmt.Printf("[SERVER] [%d] timeout for index = %d\n", kv.me, index)
		return false
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) startListeningOnApplyChannel() {

	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		fmt.Printf("[SERVER] [%d] got from applyCh op = %v\n", kv.me, op)

		kv.mu.Lock()

		if op.OpName != "Get" {
			switch op.OpName {
			case "Put":
				kv.kvStore[op.Key] = op.Value
			case "Append":
				kv.kvStore[op.Key] += op.Value
			}
		}

		ch, ok := kv.logIndexVsOpSucceededChan[msg.CommandIndex]

		if ok {
			ch <- op
		}

		kv.mu.Unlock()
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.logIndexVsOpSucceededChan = make(map[int]chan Op)

	go kv.startListeningOnApplyChannel()

	return kv
}
