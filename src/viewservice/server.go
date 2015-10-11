package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentview View
	serversPingTime map[string]time.Time
	ack bool
}

func (vs *ViewServer) updateView(primary string, backup string) bool {
	if vs.ack && (vs.currentview.Primary != primary || vs.currentview.Backup != backup) {
		DPrintf("Updating view")
		vs.currentview.Primary = primary
		vs.currentview.Backup = backup
		vs.currentview.Viewnum++;
		vs.ack = false
		return true
	}

	return false
}

func (vs *ViewServer) getNewServerForBackup() string {
	for key, _ := range vs.serversPingTime {
		//Return the server that is neither primary/backup
		if vs.currentview.Primary != key && vs.currentview.Backup != key {
			return key
		}
	}
	return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	DPrintf("Server: ", args.Viewnum)
	vs.mu.Lock()
	vs.serversPingTime[args.Me] = time.Now()
	viewnumber := args.Viewnum

	// Very first View Point as the view number is 0
	if viewnumber == 0 {
		if vs.currentview.Primary == "" && vs.currentview.Backup == "" {
 			DPrintf("inside first view")
			vs.updateView(args.Me, "")
		} else if vs.currentview.Primary == args.Me {
			DPrintf("primary has just restarted. So, promote backup as the primary");
			vs.updateView(vs.currentview.Backup, vs.getNewServerForBackup())
		} else if vs.currentview.Backup == args.Me {
			DPrintf("Backup is just restarted. So, get a new backup server")
			vs.updateView(vs.currentview.Primary, vs.getNewServerForBackup())
		}
	} else {
		DPrintf("Getting into else case")
	}

	if !vs.ack {
		if vs.currentview.Primary == args.Me && vs.currentview.Viewnum == viewnumber {
			vs.ack = true
		}
	}

	reply.View = vs.currentview
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentview
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.ack {
		for key, value := range vs.serversPingTime {
			if time.Since(value) > DeadPings * PingInterval {
				if key == vs.currentview.Primary {
					vs.updateView(vs.currentview.Backup, vs.getNewServerForBackup())
				} else if key == vs.currentview.Backup {
					vs.updateView(vs.currentview.Primary, vs.getNewServerForBackup())
				}
			}
		}


		if vs.currentview.Primary == "" {
			vs.updateView(vs.currentview.Backup, vs.getNewServerForBackup())
		} else if vs.currentview.Backup == "" {
			vs.updateView(vs.currentview.Primary, vs.getNewServerForBackup())
		}

	}



}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.ack = true
	vs.serversPingTime = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
