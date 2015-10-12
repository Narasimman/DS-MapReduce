package pbservice

import "errors"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrWrongView   = "ErrWrongViewNumber"
)

var RPCERR = errors.New("RPC Error")

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Operation string
	Client    string
	UUID      string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type SyncArgs struct {
	Data    map[string]string
	Viewnum uint
}
type SyncReply struct {
	Err Err
}
