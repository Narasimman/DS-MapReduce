package pbservice

import (
	"time"
	"viewservice"
)
import "net/rpc"
import "fmt"
import "sync"
import "strconv"

import "crypto/rand"
import "math/big"

type Clerk struct {
	vs   *viewservice.Clerk
	view viewservice.View
	mu   sync.Mutex
	// Your declarations here
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ok := false
	if !ok {
		ck.view, ok = ck.vs.Get()
		time.Sleep(viewservice.PingInterval)
	}

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if key == "" {
		return ErrNoKey
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	reply := new(GetReply)
	ok := call(ck.view.Primary, "PBServer.Get", &GetArgs{Key: key}, reply)

	for !(ok && reply.Err == OK) {
		ck.view, ok = ck.vs.Get()
		if ok {
			ok = call(ck.view.Primary, "PBServer.Get", &GetArgs{Key: key}, reply)
		}

		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	if key == "" {
		return
	}

	DPrintf("PutAppend start")

	reply := new(PutAppendReply)
	uuid := strconv.FormatInt(nrand(), 10)
	ok := call(ck.view.Primary, "PBServer.PutAppend", &PutAppendArgs{Key: key, Value: value,
		Operation: op, Client: ck.view.Primary, UUID: uuid}, reply)

	for !(ok && reply.Err == OK) {
		DPrintf("Retry PutAppend")
		ck.view, ok = ck.vs.Get()
		if ok {
			ok = call(ck.view.Primary, "PBServer.PutAppend", &PutAppendArgs{Key: key, Value: value,
				Operation: op, Client: ck.view.Primary, UUID: uuid}, reply)
		}

		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.PutAppend(key, value, "Append")
}
