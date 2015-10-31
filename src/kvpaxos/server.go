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
	if op.Type == PutOp{
		if op.OpType == "Append"{
			kv.content[op.Key] = value + op.Value
		} else if op.OpType == "Put"{
			kv.content[op.Key] = op.Value
		}
	}
	kv.processed++
	kv.px.Done(kv.processed)
}

func (kv *KVPaxos) WaitOnAgreement(seq int) Op{
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}

func (kv *KVPaxos) requestOperation(req *Op) (bool, string) {
	var ok = false
	for !ok {
		//check seen
		uuid, exists := kv.seen[op.Client]
		if exists && uuid == op.UUID {
			return false, kv.replies[op.Client]
		}
		
		seq := kv.processed + 1
		decided, t := kv.px.Status(seq)
		var res Op
		
		if decided == Decided {
			res = t.(Op)
		} else {
			kv.px.Start(seq, op)
			res = kv.WaitAgreement(seq)
		}
	
		ok = res.UUID == op.UUID
		kv.Apply(res, seq)
	}
	return true, kv.replies[op.Client]
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	kv.mu.Unlock()
	
	reqArgs := &Op {
		OpType 	: GetOp,
		Key		: args.Key,
		UUID		: args.UUID,
		Client	: args.Me,
	}
	
	
	
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	
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
	content = make(map[string]string)
	seen	 = make(map[string]int64)
	replies = make(map[string]string)
	completed = 0

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
