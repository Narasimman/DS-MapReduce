package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// Delegete jobs to workers for map.
// After Map is done, delegate jobs to workers for reduce operation.
// This also handles worker failures.
// 
func (mr *MapReduce) RunMaster() *list.List {
	mapchannel, reducechannel := make(chan int, mr.nMap), make(chan int, mr.nReduce)

	makeRPCcall := func (worker string, id int, operation JobType) bool {
		var jobArgs DoJobArgs
		var replyArgs DoJobReply
		jobArgs.File = mr.file
		jobArgs.Operation = operation
		jobArgs.JobNumber = id

		if operation == Map {
			jobArgs.NumOtherPhase = mr.nReduce	
		} else { // Reduce
			jobArgs.NumOtherPhase = mr.nMap
		}
		return call(worker, "Worker.DoJob", jobArgs, &replyArgs)
	}

	for i := 0; i < mr.nMap ; i++ {
		go func(index int) {
			for {
				var worker string
				ok := false
				select {
					case worker = <- mr.freePoolChannel:
						DPrintf("Map - Worker from free pool\n")
					case worker = <- mr.registerChannel:
						DPrintf("Map - Worker from Registered pool\n")						
				}
				ok = makeRPCcall(worker, index, Map)
				
				if(ok) {
					mapchannel <- index
					mr.freePoolChannel <- worker
					return
				}
			} // for
			
		}(i)
	}
	
	// dump all map channel indices
	for i :=0; i < mr.nMap ; i++ {
		<- mapchannel
	}
	
	close(mapchannel)
	DPrintf("Map process done!\n")
	
	// Start reduce process
	for i := 0; i < mr.nReduce ; i++ {
		go func(index int) {
			for {
				var worker string
				ok := false
				select {
					case worker = <- mr.freePoolChannel:
						DPrintf("Reduce - Worker from free pool\n")
					case worker = <- mr.registerChannel:
						DPrintf("Reduce - Worker from registration pool\n")
				}
				
				ok = makeRPCcall(worker, index, Reduce)
				
				if(ok) {
					reducechannel <- index
					mr.freePoolChannel <- worker
					return
				}
			}
		}(i)
	}
	
	// dump all reduce channel indices
	for i :=0; i < mr.nReduce ; i++ {
		<- reducechannel
	}
	
	close(reducechannel)
	DPrintf("Reduce process done!\n")
	
	return mr.KillWorkers()
}
