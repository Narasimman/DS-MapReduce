package paxos

type PrepareReqArgs struct {
	Seq int
}

type PrepareRespArgs struct {
	OK bool
}

type AcceptReqArgs struct {
	seq int
}

type AcceptResArgs struct {
	OK bool
}

type DecidedReqArgs struct {
	seq int
}

type DecidedResArgs struct {
	OK bool
}
