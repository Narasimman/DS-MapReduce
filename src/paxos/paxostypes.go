package paxos

import "sync"

type instance struct {
	MuP sync.Mutex
	N   int         //instance number
	V   interface{} //value of this instance

	MuA sync.Mutex
	N_p int         //highest prepare seen
	N_a int         //highest accept seen
	V_a interface{} //value of the highest accept seen

	MuL     sync.RWMutex
	Decided bool        //boolean that says if the value is decided
	V_d     interface{} //Decided value
}

type PrepareReqArgs struct {
	Seq int
	N   int
	Me  int
}

type PrepareRespArgs struct {
	OK  bool
	N_a int
	V_a interface{}
}

type AcceptReqArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptResArgs struct {
	OK   bool
}

type DecidedReqArgs struct {
	Seq    int
	N      int
	V      interface{}
	Dones  []int
	DoneMe int
}

type DecidedResArgs struct {
}
