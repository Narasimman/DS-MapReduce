package shardmaster

type Op struct {
	Type 	string
	GroupId int64
	Servers []string
	Shard	int
	Num		int
	UUID		int64 	
}