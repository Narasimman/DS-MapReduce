package shardmaster

import (
	"os"
	"paxos"
	"fmt"
)

import "crypto/rand"
import "math/big"

const (
	JoinOp 	= "Join"
	MoveOp 	= "Move"
	QueryOp	= "Query"
	LeaveOp	= "Leave"	
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func GetShard(gid int64, config *Config) int {
	for shard, g := range config.Shards {
		if gid == g {
			return shard
		}
	}
	return -1
}

/*
This function finds the group that has less and more number of shards 
The group that is light will be used for leave operation
	and shards will be added to the light group
The group that is heavy will be used during join operation
	and those shards will be added to the new group
*/
func FindGroupToBalance(config *Config) (int64, int64) {
	light, l_count, heavy, h_count := int64(0), int(^uint(0) >> 1), int64(0), -1
	counts := make(map[int64]int)
	
	for gid := range config.Groups {
		counts[gid] = 0
	}

	for _, gid := range config.Shards {
		counts[gid] = counts[gid] + 1
	}

	for gid := range counts {
		_, exists := config.Groups[gid]
		
		if exists && l_count > counts[gid] {
			l_count = counts[gid]
			light = gid
		}

		if exists && h_count < counts[gid] {
			h_count = counts[gid]
			heavy	= gid
		}
	}

	for _, gid := range config.Shards {
		if gid == 0 {
			heavy = 0
		}
	}

	return light, heavy
}

func (sm *ShardMaster) RebalanceShards(gid int64, operation string) {
	config := &sm.configs[sm.configNum]
	i := 0

	for {
		light, heavy := FindGroupToBalance(config)

		if operation == LeaveOp {
			DPrintf("Rebalance shard for leave operation")
			shard := GetShard(gid, config)
			
			if shard == -1 {
				break
			}

			config.Shards[shard] = light
			
		} else if operation == JoinOp {
			DPrintf("Rebalance operaion for Join Operation")
			if i == NShards / len(config.Groups) {
				break
			}

			shard := GetShard(heavy, config)
			config.Shards[shard] = gid
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
	newConfig.Groups = map[int64][]string{}
	newConfig.Shards = [NShards]int64{}
	
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	
	for shard, gid := range oldConfig.Shards {
		newConfig.Shards[shard] = gid
	}

	sm.configNum++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.configNum]	
}

func (sm *ShardMaster) CallOp(op Op, seq int) Config {
	sm.processed++
	
	gid, servers, shard, num := op.GroupId, op.Servers, op.Shard, op.Num
	switch op.Type {
		case JoinOp:
			sm.DoJoin(gid, servers)
		case MoveOp:
			sm.DoMove(shard, gid)
		case QueryOp:
			return sm.DoQuery(num)
		case LeaveOp:
			sm.DoLeave(gid)
		default:
			fmt.Println("Invalid Operation")
	}
	sm.px.Done(sm.processed)
	return Config{}
}


func (sm *ShardMaster) RequestOp(op Op) Config {
	op.UUID = nrand()

	// Loop until paxos gives a decision
	for {
		//DPrintf("Looping until paxos gives a decision") 
		seq := sm.processed + 1
		decided, val :=sm.px.Status(seq)
		var res Op
		if decided == paxos.Decided {
			res = val.(Op)
		} else {
			sm.px.Start(seq, op)
			res = sm.WaitOnAgreement(seq)
		}
		config := sm.CallOp(res, seq)

		if res.UUID == op.UUID {
			return config
		}
	}
}

func (sm *ShardMaster) isValidConfig(config Config) {
	if len(config.Groups) < 1 {
		return
	}

	DPrintf(config.Groups)

	for _, gid := range config.Shards {
		_, exists := config.Groups[gid]

		if !exists {
			fmt.Println("Invalid Configuration")
			os.Exit(-1)
		}
	}
}
