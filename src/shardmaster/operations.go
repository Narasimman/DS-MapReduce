package shardmaster

func (sm *ShardMaster) JoinHandler(gid int64, servers []string) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]

	if !exists {
		//DPrintf("Join a new group")
		config.Groups[gid] = servers
		sm.RebalanceShards(gid, JoinOp)
	}
}

func (sm *ShardMaster) LeaveHandler(gid int64) {
	config := sm.GetNextConfig()

	_, exists := config.Groups[gid]

	if exists {
		DPrintf("Leaving bye bye")
		delete(config.Groups, gid)
		sm.RebalanceShards(gid, LeaveOp)
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
	if num == -1 {
		config := sm.configs[sm.configNum]

		if len(config.Groups) < 1 {
			//Should we throw an error here???..not sure. but the cases pass.
			return Config{}
		}

		return config
	} else {
		return sm.configs[num]
	}
}
