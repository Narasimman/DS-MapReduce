package shardmaster

const (
	JoinOp 	= "Join"
	MoveOp 	= "Move"
	QueryOp	= "Query"
	LeaveOp	= "Leave"	
)

func (sm *ShardMaster) CallOp(op Op, seq int) Config {
	sm.processed++
	
	gid, servers, shard, num := op.GroupId, op.Servers, op.Shard, op.Num
	switch op.Type {
		case JoinOp:
			sm.DoJoin(gid, servers)
		case MoveOp:
		
		case QueryOp:
		
		case LeaveOp:
		
		default:
			
	}
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
		config := sm.CallOp(op, seq)
		
		if res.UUID == op.UUID {
			return config
		}
	}
}

