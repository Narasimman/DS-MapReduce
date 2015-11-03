package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOp = 1
	PutOp = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType		int
	Key			string
	Value		string
	UUID			int64
	Client		string
	Oper			string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	content		map[string]string
	seen			map[string]int64
	replies		map[string]string
	completed	int	
}

func (kv *KVPaxos) Apply(op Op, seq int) {
	value, exists := kv.content[op.Key]

	if !exists{
		value = ""
	}

	kv.replies[op.Client] = value
	kv.seen[op.Client] = op.UUID
	if op.OpType == PutOp{
		if op.Oper == "Append" {
			kv.content[op.Key] = value + op.Value
		} else if op.Oper == "Put"{
			kv.content[op.Key] = op.Value
		}
	}

	kv.completed++
	kv.px.Done(kv.completed)
}

func (kv *KVPaxos) WaitOnAgreement(seq int) Op{
	to := 10 * time.Millisecond
	var res Op
	for {
		decided, val := kv.px.Status(seq)
		if decided == paxos.Decided {
			res = val.(Op)
			return res
		}
		
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) requestOperation(req Op) (bool, string) {
	var ok = false
	for !ok {
		//check seen
		uuid, exists := kv.seen[req.Client]
		if exists && uuid == req.UUID {
			return false, kv.replies[req.Client]
		}

		seq := kv.completed + 1
		decided, t := kv.px.Status(seq)

		var res Op
		if decided == paxos.Decided {
			res = t.(Op)
		} else {
			kv.px.Start(seq, req)
			res = kv.WaitOnAgreement(seq)
		}

		ok = res.UUID == req.UUID
		kv.Apply(res, seq)
	}
	return true, kv.replies[req.Client]
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reqArgs := Op {
		OpType 	: GetOp,
		Key		: args.Key,
		UUID		: args.UUID,
		Client	: args.Me,
	}

	success, res := kv.requestOperation(reqArgs)
	
	if success {
		reply.Value = res
	} else {
		reply.Err = ErrNoKey
	}

		return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	reqArgs := Op {
		OpType	: PutOp,
		Key		: args.Key,
		Value	: args.Value,
		UUID		: args.UUID,
		Client	: args.Me,
		Oper		: args.Op,
	}
	
	success, _ := kv.requestOperation(reqArgs)
	
	if !success {
		reply.Err = ErrNoKey
	}
	
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.content = make(map[string]string)
	kv.seen	 = make(map[string]int64)
	kv.replies = make(map[string]string)
	kv.completed = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
