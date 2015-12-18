package shardkv

import (
	"net"
)
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
import "strconv"

const (
	Get         = "Get"
	Put         = "Put"
	Append      = "Append"
	Reconfigure = "Reconfigure"
)

const Debug = 0

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
	Timestamp  string //unique of the operation
	Index int   //the index of the config
	Me    string

	Config    shardmaster.Config
	Datastore map[string]string
	Logs  	  map[string] string
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
	config    shardmaster.Config
	seq       int
	datastore map[string]string
	logs 	  map[string] string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	DPrintf("Server: Get operation")

	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()


	shard := key2shard(args.Key)

	if !kv.isValidGroup(shard) {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Index: args.Index,
		Key:   args.Key,
		Op:    args.Op,
		Timestamp:  args.Timestamp,
		Me     : args.Me,
	}

	kv.RequestPaxosToUpdateDB(op)
	val, exists := kv.datastore[args.Key]

	if exists == false {
		reply.Err = ErrNoKey
	} else {
		DPrintf("GET: ", "GET SUCCESS")
		reply.Err = OK
		reply.Value = val
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	DPrintf("Server: Put/Append operation")
	
	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)

	if !kv.isValidGroup(shard) {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Index: args.Index,
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		Timestamp:  args.Timestamp,
		Me     : args.Me,
	}

	kv.RequestPaxosToUpdateDB(op)
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := kv.sm.Query(-1)

	if kv.config.Num == -1 && config.Num == 1 {
		DPrintf("Tick:", "Initial reconfiguration")
		kv.config = config
		return
	}

	//else update database until the latest config
	for i := kv.config.Num + 1; i <= config.Num; i++ {

		currentConfig := kv.sm.Query(i)

		new_datastore := map[string]string{}
		new_logs      := map[string]string{}

		//for each shard in kv
		for shard, old_gid := range kv.config.Shards {
			new_gid := currentConfig.Shards[shard]
			if old_gid != new_gid && new_gid == kv.gid {
				label := false
				//get data from all the servers of this group
				for _, server := range kv.config.Groups[old_gid] {

					args := &GetDataArgs{
						Shard: shard,
						Index: kv.config.Num,
					}

					reply := &GetDataReply{}

					ok := call(server, "ShardKV.GetShardData", args, reply)
					if ok && reply.Err == OK {
						for k, v := range reply.Datastore {
							new_datastore[k] = v
						}

						for k, v := range reply.Logs {
							val, exists := new_logs[k]
							if !(exists && val >= v) {
								new_logs[k] = v
							}
						}
						label = true
					}
				}
				if label == false && old_gid > 0 {
					return
				}
			}
		} //for each shard

		req := Op{
			Op:   Reconfigure,
			Timestamp: strconv.FormatInt(time.Now().UnixNano(), 10),
	
			Index:     i,
			Config:    currentConfig,
			Datastore: new_datastore,
			Logs     : new_logs,
		}
		kv.RequestPaxosToUpdateDB(req)
	} //outer for
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
	kv.logs      = make(map[string]string)
	kv.seq = 0

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
