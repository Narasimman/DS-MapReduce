package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Fate int
const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID int64
	Me string
}

type PutAppendReply struct {
	Err Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID int64
	Me string
}

type GetReply struct {
	Err   Err
	Value string
}
