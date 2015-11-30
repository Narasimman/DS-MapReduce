package paxos

// Handle the prepare request
func (px *Paxos) HandlePrepare(req *PrepareReqArgs, res *PrepareRespArgs) error {
	px.mu.Lock()

	ins := px.getInstance(req.Seq)

	//res.Done = px.dones[px.me]
	
	px.dones[req.Me] = req.Done

	px.mu.Unlock()

	// Learn  the decided value
	ins.MuL.RLock()

	if ins.Decided {
		DPrintf("PrepareHandler: respond decided value : ")
		res.N_a = ins.N_a
		res.V_a = ins.V_d
		res.Decided = true
		ins.MuL.RUnlock()
		return nil
	}
	ins.MuL.RUnlock()

	ins.MuA.Lock()
	defer ins.MuA.Unlock()

	//Check the incoming prepare request and accept or reject
	if req.N > ins.N_p {
		ins.N_p = req.N
	}
	
	res.N_a = ins.N_a
	res.V_a = ins.V_a
	res.OK = true
	
	return nil
}

// Handle the accept request
func (px *Paxos) HandleAccept(req *AcceptReqArgs, res *AcceptResArgs) error {
	px.mu.Lock()

	ins := px.getInstance(req.Seq)
	res.Done = px.dones[px.me]

	px.mu.Unlock()

	ins.MuA.Lock()
	defer ins.MuA.Unlock()

	res.OK = true

	if req.N >= ins.N_p {
		ins.N_p = req.N
		ins.N_a = req.N
		ins.V_a = req.V
		res.OK = true
	} else {
		res.OK = false
	}

	return nil
}

// Handle the decided request by setting the V_d value
func (px *Paxos) HandleDecided(req *DecidedReqArgs, res *DecidedResArgs) error {
	DPrintf("Handle decided value")

	px.mu.Lock()

	ins := px.getInstance(req.Seq)
	//px.dones[px.me] = req.Done
	res.Done = px.dones[px.me]

	px.mu.Unlock()

	ins.MuL.Lock()
	defer ins.MuL.Unlock()

	ins.Decided = true
	ins.V_d = req.V

	return nil
}
