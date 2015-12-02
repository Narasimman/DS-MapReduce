package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	processed int      //  processed in paxos
	configNum int      // current largest config number
}

type Op struct {
	Type    string
	GroupId int64
	Servers []string
	Shard   int
	Num     int
	UUID    int64
}

// Debugging
const Debug = 1

func DPrintf(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Println(a...)
	}
	return
}

func (sm *ShardMaster) WaitOnAgreement(seq int) Op {
	to := 10 * time.Millisecond
	var res Op
	for {
		decided, val := sm.px.Status(seq)
		if decided == paxos.Decided {
			res = val.(Op)
			return res
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Join Operation:")

	op := Op{
		Type:    JoinOp,
		GroupId: args.GID,
		Servers: args.Servers,
	}

	sm.RequestOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Leave Operation:")

	op := Op{
		Type:    LeaveOp,
		GroupId: args.GID,
	}

	sm.RequestOp(op)
	
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Move Operation:")
	op := Op{
		Type:    MoveOp,
		Shard:   args.Shard,
		GroupId: args.GID,
	}

	sm.RequestOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Query Operation:")

	op := Op{
		Type: QueryOp,
		Num:  args.Num,
	}

	reply.Config = sm.RequestOp(op)
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
