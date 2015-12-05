package shardmaster

func (sm *ShardMaster) JoinHandler(gid int64, servers []string) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]

	if !exists {
		//DPrintf("Join a new group")
		config.Groups[gid] = servers
		sm.RedistributeShards(gid, JoinOp)
	}
}

func (sm *ShardMaster) LeaveHandler(gid int64) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]

	if exists {
		DPrintf("Leaving bye bye")
		delete(config.Groups, gid)
		sm.RedistributeShards(gid, LeaveOp)
	}
}

func (sm *ShardMaster) MoveHandler(shard int, gid int64) {
	config := sm.GetNextConfig()
	config.Shards[shard] = gid
}

/*
Return the highest known configuration if num is negative
return num config otherwise
*/
func (sm *ShardMaster) QueryHandler(num int) Config {
	config := Config{}
	if num >= 0 {
		config = sm.configs[num]
	} else {
		//Return the latest configuration
		config = sm.configs[sm.configNum]
	}
	return config
}
