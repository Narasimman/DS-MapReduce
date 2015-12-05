package shardmaster

func (sm *ShardMaster) JoinHandler(gid int64, servers []string) {

	op := Op{
		Type:    JoinOp,
		GroupId: gid,
		Servers: servers,
	}

	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) LeaveHandler(gid int64) {
	op := Op{
		Type:    LeaveOp,
		GroupId: gid,
	}
	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) MoveHandler(shard int, gid int64) {
	op := Op{
		Type:    MoveOp,
		Shard:   shard,
		GroupId: gid,
	}
	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) QueryHandler(num int) Config {
	op := Op{
		Type: QueryOp,
		Num:  num,
	}
	return sm.RequestPaxosOnOp(op)
}


/*
This is the core function called by the Paxos protocol agreement
*/
func (sm *ShardMaster) PerformOperation(op Op) Config {
	operation := op.Type

	if operation == JoinOp {
		gid := op.GroupId
		servers := op.Servers
		currentconfig := sm.configs[sm.configNum]
		_, exists := currentconfig.Groups[gid]

		if !exists {
			config := sm.GetNextConfig()
			//DPrintf("Join a new group")
			config.Groups[gid] = servers
			sm.RedistributeShards(gid, JoinOp)
		}
	} else if operation == LeaveOp {
		gid := op.GroupId
		currentconfig := sm.configs[sm.configNum]
		_, exists := currentconfig.Groups[gid]

		if exists {
			config := sm.GetNextConfig()
			DPrintf("Leaving bye bye")
			delete(config.Groups, gid)
			sm.RedistributeShards(gid, LeaveOp)
		}

	} else if operation == MoveOp {
		shard := op.Shard
		gid := op.GroupId
		config := sm.configs[sm.configNum]
		current_group := config.Shards[shard]

		if current_group >= 0 {
			newconfig := sm.GetNextConfig()
			newconfig.Shards[shard] = gid
		}

	} else if operation == QueryOp {
		num := op.Num
		config := Config{}
		if num >= 0 {
			config = sm.configs[num]
		} else {
			//Return the latest configuration
			config = sm.configs[sm.configNum]
		}
		return config
	}
	return Config{}
}