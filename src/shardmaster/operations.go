package shardmaster

func (sm *ShardMaster) DoJoin(gid int64, servers []string) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]

	if !exists {
		//DPrintf("Join a new group")
		DPrintf(gid)
		config.Groups[gid] = servers
		sm.RebalanceShards(gid, JoinOp)
	}
}

func (sm *ShardMaster) DoLeave(gid int64) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]
	//DPrintf(gid)
	//DPrintf(exists)
	if exists {
		DPrintf("Leaving bye bye")
		delete(config.Groups, gid)
		sm.RebalanceShards(gid, LeaveOp)
	}
}

func (sm *ShardMaster) DoMove(shard int, gid int64) {
	config := sm.GetNextConfig()
	config.Shards[shard] = gid
}


func (sm *ShardMaster) DoQuery(num int) Config {
	if num == -1 {
		config := sm.configs[sm.configNum]
		sm.isValidConfig(config)
		return sm.configs[sm.configNum]
	} else {
		return sm.configs[num]
	}
}