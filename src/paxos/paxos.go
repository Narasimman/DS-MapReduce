package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	n_servers  int

	instances map[int]*instance //map of paxos instances for each sequence
	max_known int               //highest sequence known to this peer
	done      int               //is this peer done?
	dones     []int             //map of all dones by all paxos peers
}

// Debugging
const Debug = 0

func DPrintf(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Println(a...)
	}
	return
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	DPrintf("Start: Application starts on Paxos agreement : ")
	px.mu.Lock()
	min := px.Min()

	if seq < min {
		px.mu.Unlock()
		DPrintf("Start: ignore seq less than the min (forgotten)")
		return
	}

	ins := px.getInstance(seq)

	if seq > px.max_known {
		//update max known
		DPrintf("Max known updated")
		px.max_known = seq
	}

	px.mu.Unlock()

	DPrintf("start: Read decided boolean")
	ins.MuL.Lock()
	decided := ins.Decided
	ins.MuL.Unlock()

	if decided {
		DPrintf("Start: Value is decided")
		return
	}

	ins.MuP.Lock()
	ins.V = v
	ins.MuP.Unlock()

	DPrintf("start: calling proposer")
	go px.proposer(seq, v)
	DPrintf("Start: End of start")
}

func (px *Paxos) isMajority(counter int) bool {
	return (counter > (px.n_servers)/2)
}

func (px *Paxos) getInstance(seq int) *instance {
	ins, ok := px.instances[seq]

	if !ok {
		DPrintf("Start: instance not found")
		ins = new(instance)
		ins.Decided = false
		ins.N_p = -1
		ins.N_a = -1
		px.instances[seq] = ins
	}

	return ins
}

func (px *Paxos) proposer(seq int, v interface{}) {
	DPrintf("Proposer: start proposer")

	for {
		px.mu.Lock()
		ins, ok := px.instances[seq]

		if !ok {
			ins = px.getInstance(seq)
		}

		me := px.me
		peers := px.peers
		px.mu.Unlock()

		ins.MuP.Lock()
		ins.N = int(time.Now().UnixNano())*len(peers) + me + 1
		instanceNum := ins.N
		ins.MuP.Unlock()

		// Send Prepare message.
		// Construct req and res args
		DPrintf("Send Prepare message...")

		pinged := 0
		acceptedPrepare := 0
		max_seen, v_ := -1, v

		for i := range peers {
			prepReqArgs := &PrepareReqArgs{
				Seq: seq,
				N:   instanceNum,
				Me:  me,
			}

			prepResArgs := new(PrepareRespArgs)
			prepResArgs.OK = false

			ok = false
			if i == me {
				err := px.HandlePrepare(prepReqArgs, prepResArgs)
				if err == nil {
					pinged++
				}
			} else {
				ok = call(peers[i], "Paxos.HandlePrepare", prepReqArgs, prepResArgs)
				if ok {
					pinged++
				}
			}

			if prepResArgs.OK == true {
				acceptedPrepare++

				if prepResArgs.N_a > max_seen {
					max_seen = prepResArgs.N_a
					v_ = prepResArgs.V_a
				}
			}
		} // for prepare

		if !px.isMajority(acceptedPrepare) {
			if !px.isMajority(pinged) {
				time.Sleep(20 * time.Millisecond)
			}
			continue
		}

		acceptedCount := 0
		pinged = 0

		for i := range peers {
			accReqArgs := &AcceptReqArgs{
				Seq: seq,
				N:   instanceNum,
				V:   v_,
			}
			accResArgs := new(AcceptResArgs)
			accResArgs.OK = false

			if i == me {
				err := px.HandleAccept(accReqArgs, accResArgs)
				if err == nil {
					pinged++
				}
			} else {
				ok = call(peers[i], "Paxos.HandleAccept", accReqArgs, accResArgs)
				if ok {
					pinged++
				}
			}

			if accResArgs.OK == true {
				acceptedCount++
			}
		}

		if !px.isMajority(acceptedCount) {
			if !px.isMajority(pinged) {
				time.Sleep(20 * time.Millisecond)
			}
			continue
		}

		for i := range peers {
			decReqArgs := &DecidedReqArgs{
				Seq:    seq,
				N:      ins.N,
				V:      v_,
				Dones:  px.dones,
				DoneMe: px.me,
			}
			decResArgs := new(DecidedResArgs)

			if i == me {
				px.HandleDecided(decReqArgs, decResArgs)
			} else {
				call(peers[i], "Paxos.HandleDecided", decReqArgs, decResArgs)
			}
		}
		return
	} // infinite for
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.done < seq {
		px.done = seq
		px.dones[px.me] = seq

		for k, ins := range px.instances {
			if k <= seq && ins.Decided {
				delete(px.instances, k)
			}
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	DPrintf(px.me)
	DPrintf("Max Known is")
	DPrintf(px.max_known)
	return px.max_known
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	min := int(^uint(0) >> 1)

	for i := range px.dones {
		if min > px.dones[i] {
			min = px.dones[i]
		}
	}

	DPrintf("Min is : ")
	DPrintf(min)
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	px.mu.Lock()

	if seq < px.Min() {
		px.mu.Unlock()
		return Forgotten, nil
	}

	ins, ok := px.instances[seq]
	px.mu.Unlock()

	if !ok {
		return Pending, nil
	}

	ins.MuL.Lock()
	defer ins.MuL.Unlock()

	if !ins.Decided {
		return Pending, nil
	}

	return Decided, ins.V_d
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	DPrintf("MAKE PEER")
	// Your initialization code here.
	px.instances = make(map[int]*instance)
	px.max_known = -1
	px.done = -1
	px.dones = make([]int, len(px.peers))
	px.n_servers = len(peers)

	for k := range px.peers {
		px.dones[k] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
