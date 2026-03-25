package sharding

type Sharding struct {
	// 返回分库分表的信息
	// info 是用来分库分表的信息
	ShardingFunc        func(key any) Dst
	EffevtiveTablesFunc func() []Dst
}

type Dst struct {
	DB    string
	Table string
}
