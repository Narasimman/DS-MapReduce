package shardmaster

func (sm *ShardMaster) DoJoin(gid int64, servers []string) {
	config := sm.GetNextConfig()
	
	_, exists := config.Groups[gid]
	
	if !exists {
		config.Groups[gid] = servers
		sm.RebalanceShards(gid, JoinOp)
	}
}

func (sm *ShardMaster) DoLeave(gid int64) {
	config := sm.GetNextConfig()
	
	_, exists := config.Groups[gid]
	
	if exists {
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
		return config
	} else {
		return sm.configs[num]
	}
}