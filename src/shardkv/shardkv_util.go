package shardkv

import "time"
import "paxos"
import "strconv"

func (kv *ShardKV) WaitForAgreement(op Op) Op {
	kv.seq++
	kv.px.Start(kv.seq, op)
	to := 10 * time.Millisecond
	res := Op{}

	for { //wait for paxos majority
		status, val := kv.px.Status(kv.seq)

		if status == paxos.Decided {
			res = val.(Op)
			break
		}

		time.Sleep(to)

		if to < 10*time.Second {
			to = to * 2
		}
	}
	return res
}

func (kv *ShardKV) isValidGroup(shard int) bool {
	return kv.config.Shards[shard] == kv.gid	
}

func (kv *ShardKV) RequestPaxosToUpdateDB(op Op) {

	for { // loop until paxos succeed
		if op.Op == Reconfigure {
			//if the config number is less than the highest seen config
			if op.Config.Num <= kv.config.Num {
				DPrintf("wrong: ", "current operation config is less than highest seen")
				return
			}
		} else if op.Op == Put || op.Op == Get || op.Op == Append {
			shard := key2shard(op.Key)

			//avoid any old operation requests 
			timestamp, exists := kv.logs[op.Me + op.Op]
			if exists && timestamp >= op.Timestamp {
				return
			}

			if !kv.isValidGroup(shard) {
				DPrintf("paxos group:  ", "wrong group")
				return
			}
			
		}

		res := kv.WaitForAgreement(op)
		kv.UpdateDatastore(res)
		kv.px.Done(kv.seq)
		DPrintf("Paxos running")
		if res.Timestamp == op.Timestamp {
			return
		}
	} // infinite for to loop until paxos decides
}

/*
The core function to update the data store.

*/
func (kv *ShardKV) UpdateDatastore(op Op) {
	if op.Op == Put {
		kv.datastore[op.Key] = op.Value
		kv.logs[op.Me + op.Op] = op.Timestamp
	} else if op.Op == Append {
		kv.datastore[op.Key] += op.Value
		kv.logs[op.Me + op.Op] = op.Timestamp
	} else if op.Op == Reconfigure {
		DPrintf("Reconfigure in updatedatastore: ", "copying datastore")
		kv.config = op.Config
		for k, v := range op.Datastore {
			kv.datastore[k] = v
		}
		
		for k, v := range op.Logs {
			val, exists := kv.logs[k]
			if(!(exists && val >= v)) {
				kv.logs[k] = v
			}
		}
	} else {
		DPrintf("Update datastore", "GetData Operation. So, nothing to do")
	}
	DPrintf("Updating db ", op.Key+" --> "+kv.datastore[op.Key])
}

/*
Getting the shard data so that it can be added to the
current configuration during reconfigure operation
*/
func (kv *ShardKV) GetShardData(args *GetDataArgs, reply *GetDataReply) error {
	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	req := Op{
		Op:   "GetData",
		Timestamp: strconv.FormatInt(time.Now().UnixNano(), 10),
	}

	kv.RequestPaxosToUpdateDB(req)

	if args.Index > kv.config.Num {
		reply.Err = ErrIndex
		return nil
	}

	shard := args.Shard

	data := make(map[string]string)
	logs := make(map[string]string)

	for k, v := range kv.datastore {
		if key2shard(k) == shard {
			data[k] = v
		}
	}

	for k,v := range kv.logs {
		logs[k] = v
	}

	reply.Err = OK
	reply.Datastore = data
	reply.Logs = logs

	return nil
}
