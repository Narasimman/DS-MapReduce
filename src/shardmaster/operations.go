package shardmaster

func (sm *ShardMaster) JoinHandler(gid int64, servers []string) {

	op := Op{
		Type:    JoinOp,
		GroupId: gid,
		Servers: servers,
	}

	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) PerformJoin(op Op) {
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
}

func (sm *ShardMaster) LeaveHandler(gid int64) {
	op := Op{
		Type:    LeaveOp,
		GroupId: gid,
	}
	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) PerformLeave(op Op) {
	gid := op.GroupId
	currentconfig := sm.configs[sm.configNum]
	_, exists := currentconfig.Groups[gid]

	if exists {
		config := sm.GetNextConfig()
		DPrintf("Leaving bye bye")
		delete(config.Groups, gid)
		sm.RedistributeShards(gid, LeaveOp)
	}

}

func (sm *ShardMaster) MoveHandler(shard int, gid int64) {
	op := Op{
		Type:    MoveOp,
		Shard:   shard,
		GroupId: gid,
	}
	sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) PerformMove(op Op) {
	shard := op.Shard
	gid := op.GroupId
	config := sm.configs[sm.configNum]
	current_group := config.Shards[shard]

	if current_group >= 0 {
		newconfig := sm.GetNextConfig()
		newconfig.Shards[shard] = gid
	}
}

/*
Return the highest known configuration if num is negative
return num config otherwise
*/
func (sm *ShardMaster) QueryHandler(num int) Config {
	op := Op{
		Type: QueryOp,
		Num:  num,
	}
	return sm.RequestPaxosOnOp(op)
}

func (sm *ShardMaster) PerformQuery(op Op) Config {
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
