package shardmaster

const (
	JoinOp 	= "Join"
	MoveOp 	= "Move"
	QueryOp	= "Query"
	LeaveOp	= "Leave"	
)

func (sm *ShardMaster) PerformOp(op Op, seq int) {
	sm.processed++
	
}


func (sm *ShardMaster) RequestOp(op Op) Config {
	// Loop until paxos gives a decision
	for { 
		seq := sm.processed + 1
		decided, val :=sm.px.Status(seq)
		var res Op
		if decided == paxos.Decided {
			res = val.(Op)
		} else {
			sm.px.Start(seq, op)
			res = sm.WaitOnAgreement(seq)
		}
	}
}

