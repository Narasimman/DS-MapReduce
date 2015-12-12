package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	view      viewservice.View
	requests  map[string]string
	data      map[string]string
	pingCount int
}

// Debugging
const Debug = 1

func DPrintf(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Println(a...)
	}
	return
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	val, ok := pb.data[args.Key]

	if ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
	reply.Err = OK

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	DPrintf("Inside put append")
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.requests[args.Client] == args.UUID {
		DPrintf("Duplicate Request")
		reply.Err = OK
		return nil
	}

	if pb.view.Backup != "" {
		DPrintf("Put into the backup first")
		ok := call(pb.view.Backup, "PBServer.Syncbackup", args, reply)

		if !ok {
			return RPCERR
		}

		if reply.Err != OK {
			return nil
		}
	}

	pb.requests[args.Client] = args.UUID

	if args.Operation == "Append" {
		pb.data[args.Key] = pb.data[args.Key] + args.Value
	} else {
		pb.data[args.Key] = args.Value
	}

	reply.Err = OK

	return nil
}

/*
RPC Backup sync function called by PutAppend. The primary first makes call to
sync with backup and only after this rpc call succeeds and the primary get the ack
it puts into its database and proceeds.
*/
func (pb *PBServer) Syncbackup(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.requests[args.Client] == args.UUID {
		DPrintf("Duplicate Request")
		reply.Err = OK
		return nil
	}

	if args.Operation == "Append" {
		pb.data[args.Key] = pb.data[args.Key] + args.Value
	} else {
		pb.data[args.Key] = args.Value
	}

	reply.Err = OK

	return nil
}

/*
This RPC function is to perform a complete state transfer from primary to backup
this is called in the tick function when the backup is reset and needs a state transfer before
it can acts as a fully functional backup
*/

func (pb *PBServer) SyncAll(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum != pb.view.Viewnum {
		reply.Err = ErrWrongView
		return nil
	}

	DPrintf("Complete state transfer!")
	pb.data = args.Data
	reply.Err = OK

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.pingCount > viewservice.DeadPings {
		pb.view.Viewnum = 0
		pb.view.Primary = ""
		pb.view.Backup = ""
		pb.pingCount = 0
	} else {
		view, err := pb.vs.Ping(pb.view.Viewnum)
		if err == nil {
			pb.pingCount = 0
			if view.Primary == pb.me && view.Backup != "" && view.Backup != pb.view.Backup {
				DPrintf("Sync whole database with backup")
				reply := new(SyncReply)
				ok := call(view.Backup, "PBServer.SyncAll", &SyncArgs{Data: pb.data, Viewnum: view.Viewnum}, reply)
				if ok && reply.Err == OK {
					pb.view = view
				}
			} else {
				DPrintf("Update the view for this server")
				pb.view = view
			}
		} else {
			pb.pingCount++
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	var err error
	pb.view, err = pb.vs.Ping(0)
	for err != nil {
		pb.view, err = pb.vs.Ping(0)
		time.Sleep(viewservice.PingInterval)
	}
	pb.data = make(map[string]string)
	pb.requests = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
