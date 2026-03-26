package local_msg_tab

import (
	"local_msg_tab/internal/service"
	"local_msg_tab/internal/sharding"

	"github.com/IBM/sarama"
	"github.com/meoying/dlock-go"
	dredis "github.com/meoying/dlock-go/redis"
	"github.com/redis/go-redis/v9"
	"github.com/rermrf/emo/logger"
	"gorm.io/gorm"
)

// NewDefaultService 默认配置，不支持分库分表的本地消息表service
func NewDefaultService(
	db *gorm.DB,
	rdb redis.Cmdable,
	producer sarama.SyncProducer,
	logger logger.Logger,
	opts ...service.ShardingServiceOpt,
) (*service.Service, error) {
	dbs := map[string]*gorm.DB{
		"": db,
	}
	lockClient := dredis.NewClient(rdb)
	return &service.Service{
		ShardingService: NewDefaultShardingService(dbs, producer, lockClient, sharding.NewNoShard("local_msgs"), logger, opts...),
	}, nil
}

// NewDefaultShardingService 初始化一个支持分库分表的 service
func NewDefaultShardingService(dbs map[string]*gorm.DB, producer sarama.SyncProducer, lockClient dlock.Client, sharding sharding.Sharding, logger logger.Logger, opts ...service.ShardingServiceOpt) *service.ShardingService {
	return service.NewShardingService(dbs, producer, lockClient, sharding, logger, opts...)
}
