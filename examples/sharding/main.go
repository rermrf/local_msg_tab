package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bwmarrin/snowflake"
	dredis "github.com/meoying/dlock-go/redis"
	"github.com/redis/go-redis/v9"
	"github.com/rermrf/emo/logger"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	lmt "local_msg_tab"
	"local_msg_tab/internal/sharding"
	"local_msg_tab/mock_biz/sharding_order"
)

// 分片策略：2 库 2 表
// buyer % 2 → 选库，buyer / 2 % 2 → 选表
var (
	dbNames    = []string{"shard_0", "shard_1"}
	tableNames = []string{"local_msgs_0", "local_msgs_1"}
)

func newSharding() sharding.Sharding {
	return sharding.Sharding{
		ShardingFunc: func(key any) sharding.Dst {
			k := key.(int64)
			return sharding.Dst{
				DB:    dbNames[k%2],
				Table: tableNames[(k/2)%2],
			}
		},
		EffectiveTablesFunc: func() []sharding.Dst {
			dsts := make([]sharding.Dst, 0, 4)
			for _, db := range dbNames {
				for _, table := range tableNames {
					dsts = append(dsts, sharding.Dst{DB: db, Table: table})
				}
			}
			return dsts
		},
	}
}

func main() {
	// 1. 初始化多个 MySQL 数据库连接（模拟分库）
	dbs := make(map[string]*gorm.DB, len(dbNames))
	for _, dbName := range dbNames {
		db, err := gorm.Open(mysql.Open(
			fmt.Sprintf("root:root@tcp(localhost:3306)/%s?charset=utf8&parseTime=True&loc=Local", dbName),
		))
		if err != nil {
			log.Fatalf("连接数据库 %s 失败: %v", dbName, err)
		}
		dbs[dbName] = db
	}

	// 2. 初始化 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 3. 初始化 Kafka SyncProducer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("创建 Kafka Producer 失败: %v", err)
	}
	defer producer.Close()

	// 4. 初始化分布式锁客户端
	lockClient := dredis.NewClient(rdb)

	// 5. 初始化本地消息表 Service（分库分表）
	l := logger.NewNopLogger()
	svc := lmt.NewDefaultShardingService(dbs, producer, lockClient, newSharding(), l)

	// 6. 初始化雪花算法节点（用于生成订单 ID）
	node, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatalf("创建雪花算法节点失败: %v", err)
	}

	// 7. 创建业务服务
	orderSvc := sharding_order.NewOrderService(node, svc)

	// 8. 启动异步补偿任务
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = svc.StartAsyncTask(ctx)
	if err != nil {
		log.Fatalf("启动异步补偿任务失败: %v", err)
	}
	fmt.Println("异步补偿任务已启动（4 个分片表各一个 goroutine）")

	// 9. 模拟创建订单（不同 buyer 会路由到不同分片）
	for i := int64(0); i < 4; i++ {
		buyer := i
		sn := fmt.Sprintf("ORDER_%d_%d", buyer, time.Now().UnixMilli())
		err = orderSvc.CreateOrder(ctx, sn, buyer)
		if err != nil {
			log.Printf("创建订单失败 buyer=%d（消息可能由补偿任务重试）: %v", buyer, err)
		} else {
			fmt.Printf("创建订单成功: buyer=%d, sn=%s → 路由到 %s/%s\n",
				buyer, sn, dbNames[buyer%2], tableNames[(buyer/2)%2])
		}
	}

	// 10. 等待信号退出
	fmt.Println("服务运行中，按 Ctrl+C 退出...")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	fmt.Println("服务已退出")
}
