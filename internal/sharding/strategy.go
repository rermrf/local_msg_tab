package sharding

func NewNoShard(table string) Sharding {
	return Sharding{
		ShardingFunc: func(msg any) Dst {
			return Dst{
				Table: table,
			}
		},
		EffevtiveTablesFunc: func() []Dst {
			return []Dst{
				{
					Table: table,
				},
			}
		},
	}
}
