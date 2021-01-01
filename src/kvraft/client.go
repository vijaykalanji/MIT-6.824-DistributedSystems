package raftkv

import (
	"fmt"
	"labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	//Caching the current leader for faster responses.
	currentLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.currentLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	///Create the request. The structure is defined in server.go
	args := GetArgs{Key: key}
	fmt.Println("[CLERK] GET key =", key)
	for {
		reply := GetReply{}
		fmt.Printf("[CLERK] trying server %d\n", ck.currentLeader)
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply)
		fmt.Printf("[CLERK] server returned res=%t reply=%+v\n", ok, reply)
		if ok && !reply.WrongLeader {
			fmt.Println("---------------------------------------------------------------------------")
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value
		}
		//Consider the next peer as leader and then give it a try.
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	fmt.Printf("[CLERK] PA args = %+v\n", args)
	for {
		reply := PutAppendReply{}
		fmt.Printf("[CLERK] trying server %d\n", ck.currentLeader)
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)
		fmt.Printf("[CLERK] server returned res=%t reply=%+v\n", ok, reply)
		if ok && !reply.WrongLeader {
			fmt.Println("---------------------------------------------------------------------------")
			return
		}
		//Consider the next peer as leader and then give it a try.
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
