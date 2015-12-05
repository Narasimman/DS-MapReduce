package shardmaster

import (
	"fmt"
	"paxos"
	"time"
)

import "crypto/rand"
import "math/big"

const (
	JoinOp  = "Join"
	MoveOp  = "Move"
	QueryOp = "Query"
	LeaveOp = "Leave"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

/*
The usual wait agreement for the paxos protocol
*/
func (sm *ShardMaster) WaitOnAgreement(seq int) Op {
	to := 10 * time.Millisecond
	var res Op
	for {
		decided, val := sm.px.Status(seq)
		if decided == paxos.Decided {
			res = val.(Op)
			return res
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

/*
Returns the shard for a given group id and config.
*/
func getShard(gid int64, config *Config) int {
	for shard, g := range config.Shards {
		if gid == g {
			return shard
		}
	}
	return -1
}

func getMinGroup(config *Config, countMap map[int64]int) int64 {
	group := int64(0)
	count := int(^uint(0) >> 1)
	for gid := range countMap {
		_, exists := config.Groups[gid]
		if exists {
			if count > countMap[gid] {
				count = countMap[gid]
				group = gid
			}
		}
	}
	return group
}

func getMaxGroup(config *Config, countMap map[int64]int) int64 {
	group := int64(0)
	count := -1

	for gid := range countMap {
		_, exists := config.Groups[gid]
		if exists {
			if count < countMap[gid] {
				count = countMap[gid]
				group = gid
			}
		}
	}
	return group
}

func isEmptyGroup(gid int64, config *Config) bool {
	for _, g := range config.Shards {
		if gid == g {
			return false
		}
	}
	return true
}

func getShardCountPerGroup(config *Config) map[int64]int {
	shardsCount := make(map[int64]int)

	for gid := range config.Groups {
		shardsCount[gid] = 0
	}

	for _, gid := range config.Shards {
		if gid == 0 {
			return map[int64]int{}
		}

		_, exists := shardsCount[gid]

		if !exists {
			shardsCount[gid] = 0
		} else {
			shardsCount[gid]++
		}
	}

	return shardsCount
}

/*
Rebalances the shards
*/
func (sm *ShardMaster) RedistributeShards(gid int64, operation string) {
	config := &sm.configs[sm.configNum]
	shardsPerGroup := NShards / len(config.Groups)
	shardsCountMap := getShardCountPerGroup(config)
	processed := false

	if len(shardsCountMap) < 1 {
		processed = true

	}

	group := int64(0)

	for i := 0; ; i++ {
		if operation == LeaveOp {
			if isEmptyGroup(gid, config) {
				//this means we are done with redistributing all shards in the
				//group that is leaving
				DPrintf("Shards redistributed in leave group")
				return
			}

			if !processed {
				group = getMinGroup(config, shardsCountMap)
			}

			shard := getShard(gid, config)
			config.Shards[shard] = group

		} else if operation == JoinOp {
			if i < shardsPerGroup {

				if !processed {
					group = getMaxGroup(config, shardsCountMap)
				}

				if !isEmptyGroup(group, config) {
					shard := getShard(group, config)
					config.Shards[shard] = gid
				}
			} else {
				// we are done redistributing
				return
			}
		} else {
			DPrintf("Calling rebalancing for invalid operation")
		}
	}
}

func (sm *ShardMaster) GetNextConfig() *Config {
	config := &sm.configs[sm.configNum]

	var newConfig Config
	newConfig.Num = config.Num + 1
	newConfig.Groups = make(map[int64][]string)
	newConfig.Shards = [NShards]int64{}

	//Copy all groups data because assigning just gets the reference
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = servers
	}

	//copy all shards data
	for shard, gid := range config.Shards {
		newConfig.Shards[shard] = gid
	}

	sm.configNum++
	//append the new config
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.configNum]
}

/*
Calls the corresponding handler based on op arguments.
*/
func (sm *ShardMaster) CallHandler(op Op) Config {
	switch op.Type {
	case JoinOp:
		sm.JoinHandler(op.GroupId, op.Servers)
	case MoveOp:
		sm.MoveHandler(op.Shard, op.GroupId)
	case QueryOp:
		return sm.QueryHandler(op.Num)
	case LeaveOp:
		sm.LeaveHandler(op.GroupId)
	default:
		fmt.Println("Invalid Operation")
	}
	return Config{}
}

func (sm *ShardMaster) RequestPaxosOnOp(op Op) Config {
	op.UUID = nrand()

	// Loop until paxos gives a decision
	for {
		//DPrintf("Looping until paxos gives a decision")
		seq := sm.processed + 1
		decided, val := sm.px.Status(seq)
		var res Op

		if decided == paxos.Decided {
			res = val.(Op)
		} else {
			sm.px.Start(seq, op)
			res = sm.WaitOnAgreement(seq)
		}

		config := sm.CallHandler(res)
		sm.processed++
		sm.px.Done(sm.processed)

		if res.UUID == op.UUID {
			return config
		}
	}
}
