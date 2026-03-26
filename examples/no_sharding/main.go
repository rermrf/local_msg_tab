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
	"github.com/redis/go-redis/v9"
	"github.com/rermrf/emo/logger"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	lmt "local_msg_tab"
	"local_msg_tab/mock_biz/nosharding_order"
)

func main() {
	// 1. 初始化 MySQL
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:3306)/test?charset=utf8&parseTime=True&loc=Local"))
	if err != nil {
		log.Fatalf("连接 MySQL 失败: %v", err)
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

	// 4. 初始化本地消息表 Service（非分库分表）
	l := logger.NewNopLogger()
	svc, err := lmt.NewDefaultService(db, rdb, producer, l)
	if err != nil {
		log.Fatalf("创建 Service 失败: %v", err)
	}

	// 5. 创建业务服务
	orderSvc := nosharding_order.NewOrderService(db, svc)

	// 6. 启动异步补偿任务
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = svc.StartAsyncTask(ctx)
	if err != nil {
		log.Fatalf("启动异步补偿任务失败: %v", err)
	}
	fmt.Println("异步补偿任务已启动")

	// 7. 模拟创建订单
	sn := fmt.Sprintf("ORDER_%d", time.Now().UnixMilli())
	err = orderSvc.CreateOrder(ctx, sn)
	if err != nil {
		log.Printf("创建订单失败（消息可能由补偿任务重试）: %v", err)
	} else {
		fmt.Printf("创建订单成功: %s\n", sn)
	}

	// 8. 等待信号退出
	fmt.Println("服务运行中，按 Ctrl+C 退出...")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	fmt.Println("服务已退出")
}
