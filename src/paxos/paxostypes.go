package paxos

type PrepareReqArgs struct {
	Seq int
	N   int
	Done bool
	Me int
}

type PrepareRespArgs struct {
	OK bool
	N_a int
	V_a interface{}
	Done bool
}

type AcceptReqArgs struct {
	Seq int
	N   int
	V interface{}
}

type AcceptResArgs struct {
	OK bool
}

type DecidedReqArgs struct {
	seq int
	V   interface{}
}

type DecidedResArgs struct {
	OK bool
}
