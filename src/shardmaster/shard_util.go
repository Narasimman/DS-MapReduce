package shardmaster

import (
	"fmt"
	"paxos"
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

/*
This function finds the group that has less and more number of shards
The group that has less number of shares than typical will be used for leave operation
and shards will be added to the light group
The group that has more number of shards will be used during join operation
and those shards will be added to the new group
*/
func findGroupToBalance(config *Config, operation string) int64 {
	group, l_count, j_count := int64(0), int(^uint(0)>>1), -1
	counts := make(map[int64]int)

	for gid := range config.Groups {
		counts[gid] = 0
	}

	for _, gid := range config.Shards {
		counts[gid] = counts[gid] + 1
	}

	for gid := range counts {
		_, exists := config.Groups[gid]

		if exists {
			if operation == LeaveOp {
				if l_count > counts[gid] {
					l_count = counts[gid]
					group = gid
				}
			} else if operation == JoinOp {
				if j_count < counts[gid] {
					j_count = counts[gid]
					group = gid
				}
			}
		}
	}

	// if the group id is 0 then return 0 and not the max/min
	for _, gid := range config.Shards {
		if gid == 0 {
			group = 0
		}
	}

	return group
}

/*
Rebalances the shards
*/
func (sm *ShardMaster) RebalanceShards(gid int64, operation string) {
	config := &sm.configs[sm.configNum]

	i := 0
	shardsPerGroup := NShards / len(config.Groups)

	for {
		group := findGroupToBalance(config, operation)
		if operation == LeaveOp {
			//if leaving get shard from that group and 
			// distribute it to the light group
			shard := getShard(gid, config)

			if shard == -1 {
				DPrintf("Shards redistributed")
				break
			}

			config.Shards[shard] = group

		} else if operation == JoinOp {
			if i < shardsPerGroup {
				shard := getShard(group, config)

				if shard != -1 {
					config.Shards[shard] = gid
				}
			} else {
				// we are done redistributing
				break
			}
		} else {
			DPrintf("Calling rebalancing for invalid operation")
		}
		i++
	}
}

func (sm *ShardMaster) GetNextConfig() *Config {
	oldConfig := &sm.configs[sm.configNum]

	var newConfig Config
	newConfig.Num = oldConfig.Num + 1
	newConfig.Groups = make(map[int64][]string)
	newConfig.Shards = [NShards]int64{}

	//Copy all groups data
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	//copy all shards data
	for shard, gid := range oldConfig.Shards {
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
func (sm *ShardMaster) CallOp(op Op, seq int) Config {
	gid, servers, shard, num := op.GroupId, op.Servers, op.Shard, op.Num
	switch op.Type {
	case JoinOp:
		sm.JoinHandler(gid, servers)
	case MoveOp:
		sm.MoveHandler(shard, gid)
	case QueryOp:
		return sm.QueryHandler(num)
	case LeaveOp:
		sm.LeaveHandler(gid)
	default:
		fmt.Println("Invalid Operation")
	}
	return Config{}
}

func (sm *ShardMaster) RequestOp(op Op) Config {
	op.UUID = nrand()

	// Loop until paxos gives a decision
	for {
		//DPrintf("Looping until paxos gives a decision")
		sm.processed++
		seq := sm.processed
		decided, val := sm.px.Status(seq)
		var res Op

		if decided == paxos.Decided {
			res = val.(Op)
		} else {
			sm.px.Start(seq, op)
			res = sm.WaitOnAgreement(seq)
		}

		config := sm.CallOp(res, seq)
		sm.px.Done(sm.processed)

		if res.UUID == op.UUID {
			return config
		}
	}
}
