package paxos

func (px *Paxos) sendPrepare(seq int, instanceNum int) (bool, int, interface{}, int) {
	px.mu.Lock()
	ins, ok := px.instances[seq]

	me := px.me
	done := px.done
	peers := px.peers
	px.mu.Unlock()

	ins.MuA.Lock()
	max_seen := ins.N_p
	v_ := ins.V
	ins.MuA.Unlock()

	// Send Prepare message.
	// Construct req and res args
	DPrintf("Send Prepare message...")
	prepReqArgs := &PrepareReqArgs{
		Seq: seq,
		N:   ins.N,
		Done : done,
		Me:  me,
	}

	prepResArgs := new(PrepareRespArgs)

	pinged := 0
	acceptedPrepare := 0

	for i := range peers {
		ok = false
		if i == me {
			err := px.HandlePrepare(prepReqArgs, prepResArgs)
			if err == nil {
				pinged++
				ok = true
			}
		} else {
			ok = call(peers[i], "Paxos.HandlePrepare", prepReqArgs, prepResArgs)
			if ok {
				pinged++
			}
		}

		if prepResArgs.Decided {
			// Learn the decided value and abort.
			// Because the value sent by this proposer was different from the
			// value that was decided by consensus.
			ins.MuL.RLock()

			ins.Decided = true
			ins.V_d = prepResArgs.V_a
			ins.MuL.RUnlock()
			return prepResArgs.Decided, 0, prepResArgs.V_a, 0
		}

		if prepResArgs.OK {
			acceptedPrepare++

			if prepResArgs.N_a > max_seen {
				max_seen = prepResArgs.N_a

				if prepResArgs.V_a != nil {
					v_ = prepResArgs.V_a
				}
			}
		}

	} // for
	return false, acceptedPrepare, v_, pinged
}

func (px *Paxos) sendAccept(seq int, instanceNum int, v_ interface{}) int {
	px.mu.Lock()
	me := px.me
	peers := px.peers

	px.mu.Unlock()

	//Now Send Accept
	DPrintf("Send Accept message..")
	accReqArgs := &AcceptReqArgs{
		Seq: seq,
		N:   instanceNum,
		V:   v_,
	}
	accResArgs := new(AcceptResArgs)
	acceptedCount := 0

	for i := range peers {
		if i == me {
			px.HandleAccept(accReqArgs, accResArgs)
		} else {
			call(peers[i], "Paxos.HandleAccept", accReqArgs, accResArgs)
		}

		if accResArgs.OK {
			acceptedCount++
		}
	}

	return acceptedCount
}

func (px *Paxos) sendDecision(seq int, v_ interface{}) {
	DPrintf("Send decided value. Consensus has been reached!")

	px.mu.Lock()

	me := px.me
	peers := px.peers
	dones := px.dones

	px.mu.Unlock()

	decReqArgs := &DecidedReqArgs{
		Seq:    seq,
		V:      v_,
	}

	decResArgs := new(DecidedResArgs)

	for i := range peers {
		if i == me {
			px.HandleDecided(decReqArgs, decResArgs)
		} else {
			call(peers[i], "Paxos.HandleDecided", decReqArgs, decResArgs)
		}

		if dones[i] < decResArgs.Done {
			dones[i] = decResArgs.Done
		}
	}
	
	stat, _ := px.Status(seq)
	if (stat == Decided) {//while not decided
		return 
	} else {
		px.proposer(seq)
	}
}
