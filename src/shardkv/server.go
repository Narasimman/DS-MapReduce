package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const (
	Get         = "Get"
	Put         = "Put"
	Append      = "Append"
	GetData     = "GetData"
	Reconfigure = "Reconfigure"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key   string
	Value string
	Op    string
	Ts    string //time stamp of the operation
	Me    string //id of the client
	Index int    //the index of the config

	Config    shardmaster.Config
	Datastore map[string]string
	Logs      map[string]string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	Me        string // client id
	config    shardmaster.Config
	index     int
	seq       int
	datastore map[string]string
	logs      map[string]string
}

func (kv *ShardKV) RequestDatastore(op Op) {
	for {// loop until paxos succeed
		if op.Op == Reconfigure {
			if op.Config.Num <= kv.config.Num {
				return
			}
		}
		
		if op.Op != GetData {
			//check for valid group id and timestamp validation
			
		}
		
		kv.seq++
		kv.px.Start(kv.seq,op)
		to := 10 * time.Millisecond
		res := Op{}
		
		for {//wait for paxos majority
			status, val := kv.px.Status(kv.seq)

			if status == paxos.Decided {
				res = val.(Op)
				break
			}

			time.Sleep(to)

			if to < 10 * time.Second {
				to = to * 2
			}
		}

		kv.UpdateDatastore(res)
		kv.px.Done(kv.seq)
		
		if res.Ts == op.Ts {
			return
		}
		
		
		
	}// infinite for to loop until paxos decides
}

func (kv *ShardKV) UpdateDatastore(op Op) {
	
	if op.Op == Put {
		kv.datastore[op.Key] = op.Value
		
		//Update the time stamp, but what will be 
		
		kv.logs[op.Me + op.Ts] = op.Ts
	} else if op.Op  == Append {
		value := kv.datastore[op.Key]
		kv.datastore[op.Key] = value + op.Value
		
		//Should we update the time stamp here also???
		
	}
	
	DPrintf("Updating db ", op.Key + " --> " + kv.datastore[op.Key])	
	DPrintf("", len(kv.datastore))
	
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	DPrintf("Server: Get operation")
	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	kv.mu.Lock()
	kv.mu.Unlock()

	shard := key2shard(args.Key)

	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Index: args.Index,
		Key:   args.Key,
		Op:    args.Op,
		Me:    args.Me,
		Ts:    args.Ts,
	}

	kv.RequestDatastore(op)
	DPrintf("", len(kv.datastore))
	val, exists := kv.datastore[args.Key]

	if exists == false {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = val
	}

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	DPrintf("Server: Put/Append operation")
	DPrintf("index in put", args.Index)
	DPrintf("config num in put", kv.config.Num)
	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	kv.mu.Lock()
	kv.mu.Unlock()

	shard := key2shard(args.Key)

	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Index	: args.Index,
		Key		: args.Key,
		Value 	: args.Value,
		Op		: args.Op,
		Me		: args.Me,
		Ts		: args.Ts,
	}

	kv.RequestDatastore(op)

	reply.Err = OK
	
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	config := kv.sm.Query(-1)
	
	//if(kv.config.Num == -1) {
		kv.config = config
	//}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{Num: -1}
	kv.datastore = make(map[string]string)
	kv.logs = make(map[string]string)
	kv.seq = 0
	kv.index = -1

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
