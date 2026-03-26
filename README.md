# local_msg_tab - 通用本地消息表

一个基于 Go 实现的通用本地消息表（Transactional Outbox Pattern）库，支持分库分表，保证业务操作与消息发送的最终一致性。

## 适用场景

### 推荐使用

- **订单 + 消息通知**：创建订单后需要发送消息到下游（物流、支付、积分等），要求订单创建和消息发送最终一致
- **数据同步**：业务数据变更后需要同步到其他系统（搜索引擎、缓存、数据仓库），不允许丢消息
- **事件驱动架构**：微服务之间通过事件通信，需要保证事件一定能发出去
- **已有 MySQL + Kafka 技术栈**：无需引入额外中间件，利用现有基础设施即可实现可靠消息投递
- **分库分表业务**：业务数据已经分库分表，消息表需要和业务表在同一个库中以保证事务

### 不推荐使用

- **强一致性要求**：本库保证的是最终一致性（at-least-once），如果业务要求严格的分布式事务（如转账扣款），应考虑 TCC、Saga 等方案
- **消息顺序性要求极高**：补偿任务重试可能打乱消息的原始顺序，如果业务强依赖消息顺序，需要在消费端额外处理
- **超高吞吐 + 超低延迟**：每条消息都涉及一次数据库写入，在极端高吞吐场景下数据库可能成为瓶颈
- **无 MySQL 环境**：本库依赖 MySQL 事务，如果业务使用 NoSQL 或其他不支持事务的数据库，无法使用
- **消息不允许重复**：本库是 at-least-once 语义，消息可能重复投递。如果业务无法做到消费幂等，不适合使用

## 原理

本地消息表是一种经典的分布式事务解决方案，核心思想是**将消息持久化与业务操作放在同一个数据库事务中**，从而避免分布式事务的复杂性。

### 工作流程

```
业务方调用 ExecTx()
        |
        v
  ┌─────────────────────────┐
  │    数据库事务（同一个DB）    │
  │  1. 执行业务逻辑（如创建订单） │
  │  2. 插入本地消息记录（Init）   │
  └─────────────────────────┘
        |
        | 事务提交成功
        v
  尝试立即发送消息到 Kafka
        |
   ┌────┴────┐
   |         |
 成功       失败
   |         |
更新状态    仅记录日志，不影响业务
Success    消息仍为 Init 状态
             |
             v
    异步补偿任务定期扫描
    找到超时的 Init 消息
             |
        重试发送
        |
   ┌────┴────┐
   |         |
 成功     失败且未达上限
   |         |
 Success   保持 Init，下次继续
             |
          达到最大重试次数
             |
           Fail（终态）
```

### 关键设计

- **最终一致性**：业务事务提交后，消息一定会被发送（成功或最终标记失败）
- **at-least-once**：消息可能被重复发送，消费端需要保证幂等性
- **事务隔离**：消息发送失败不会回滚业务事务，业务操作已经成功

## 架构

```
 业务代码（如 OrderService）
        |
        | 调用 ExecTx(ctx, [shardingKey], bizFunc)
        v
 ┌──────────────────┐       ┌───────────────────────┐
 │     Service       │       │   ShardingService      │
 │  （非分库分表封装）  │──────>│   （核心实现）            │
 └──────────────────┘       └───────────────────────┘
                                  |            |
                 execTx:          |            | StartAsyncTask
                 DB事务 +         |            |      |
                 插入LocalMsg +   |            v      v
                 发送Kafka        |     ┌─────────────────┐
                                  |     │   AsyncTask      │
                                  |     │  (每个分片表一个)  │
                                  |     │  + 分布式锁       │
                                  |     └─────────────────┘
                                  |            |
                                  |            v
                                  |     ┌─────────────────┐
                                  |     │    Executor      │
                                  |     ├─────────────────┤
                                  |     │ CurMsgExecutor   │  并行 1:1 发送
                                  |     │ BatchMsgExecutor │  批量 SendMessages
                                  |     └─────────────────┘
                                  |
 ┌──────────┐              ┌──────────┐
 │  MySQL   │              │  Kafka   │  <── sarama.SyncProducer
 │ LocalMsg │              └──────────┘
 └──────────┘

 ┌────────────────────┐
 │ sharding.Sharding  │  ShardingFunc(key) → Dst{DB, Table}
 │                    │  EffectiveTablesFunc() → []Dst
 └────────────────────┘
```

## 特性

- **分库分表支持**：通过自定义 `Sharding` 策略，灵活路由到不同的数据库和表
- **非分库分表支持**：开箱即用的单库单表模式
- **异步补偿**：后台 goroutine 自动扫描并重试发送失败的消息
- **分布式锁**：基于 Redis 的分布式锁，避免多实例重复补偿
- **两种发送模式**：并行逐条发送（默认）和批量发送
- **可配置重试策略**：支持自定义等待时间、最大重试次数、批次大小

## 依赖

| 依赖 | 说明 |
|------|------|
| MySQL（GORM） | 存储本地消息记录 |
| Redis | 分布式锁（补偿任务去重） |
| Kafka（sarama） | 消息发送目标 |

## 数据库表结构

```sql
CREATE TABLE `local_msgs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `key` varchar(255) DEFAULT NULL,
  `data` text,
  `status` tinyint DEFAULT '0' COMMENT '0-Init 1-Success 2-Fail',
  `send_times` int DEFAULT '0',
  `ctime` bigint DEFAULT NULL,
  `utime` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_key` (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

> 分库分表场景下，每个分片数据库中需要按自定义表名创建对应的消息表。

## 快速开始

### 非分库分表

```go
package main

import (
    "context"
    "log"

    "github.com/IBM/sarama"
    "github.com/redis/go-redis/v9"
    "github.com/rermrf/emo/logger"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"

    lmt "local_msg_tab"
)

func main() {
    // 初始化依赖
    db, _ := gorm.Open(mysql.Open("root:root@tcp(localhost:3306)/test"))
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    producer, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, config)

    // 创建 Service
    svc, _ := lmt.NewDefaultService(db, rdb, producer, logger.NewNopLogger())

    ctx := context.Background()

    // 在事务中执行业务逻辑并发送消息
    err := svc.ExecTx(ctx, func(tx *gorm.DB) (lmt.Msg, error) {
        // 在这里执行你的业务逻辑，比如创建订单
        // tx 是事务中的 DB 实例，确保业务操作和消息在同一事务中
        err := tx.Create(&YourOrder{SN: "ORDER_001"}).Error
        if err != nil {
            return lmt.Msg{}, err
        }

        // 返回要发送的消息
        return lmt.Msg{
            Key:     "ORDER_001",
            Topic:   "order_created",
            Content: "订单内容",
        }, nil
    })
    if err != nil {
        log.Printf("执行失败: %v", err)
    }

    // 启动异步补偿任务
    _ = svc.StartAsyncTask(ctx)
}
```

### 分库分表

```go
package main

import (
    "context"
    "fmt"

    "github.com/IBM/sarama"
    dredis "github.com/meoying/dlock-go/redis"
    "github.com/redis/go-redis/v9"
    "github.com/rermrf/emo/logger"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"

    lmt "local_msg_tab"
    "local_msg_tab/internal/sharding"
)

func main() {
    // 初始化多个数据库连接
    dbs := map[string]*gorm.DB{}
    for _, name := range []string{"shard_0", "shard_1"} {
        db, _ := gorm.Open(mysql.Open(
            fmt.Sprintf("root:root@tcp(localhost:3306)/%s", name),
        ))
        dbs[name] = db
    }

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    lockClient := dredis.NewClient(rdb)

    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    producer, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, config)

    // 定义分片策略
    s := sharding.Sharding{
        ShardingFunc: func(key any) sharding.Dst {
            // key 是你的分片键（如 buyerID）
            k := key.(int64)
            return sharding.Dst{
                DB:    fmt.Sprintf("shard_%d", k%2),
                Table: fmt.Sprintf("local_msgs_%d", (k/2)%2),
            }
        },
        EffectiveTablesFunc: func() []sharding.Dst {
            // 返回所有分片表，用于异步补偿任务遍历
            var dsts []sharding.Dst
            for _, db := range []string{"shard_0", "shard_1"} {
                for _, table := range []string{"local_msgs_0", "local_msgs_1"} {
                    dsts = append(dsts, sharding.Dst{DB: db, Table: table})
                }
            }
            return dsts
        },
    }

    // 创建分库分表 Service
    svc := lmt.NewDefaultShardingService(dbs, producer, lockClient, s, logger.NewNopLogger())

    ctx := context.Background()

    // ExecTx 的第二个参数是分片键
    buyerID := int64(42)
    _ = svc.ExecTx(ctx, buyerID, func(tx *gorm.DB) (lmt.Msg, error) {
        // 业务逻辑...
        return lmt.Msg{
            Key:     "ORDER_042",
            Topic:   "order_created",
            Content: "订单内容",
        }, nil
    })

    // 启动异步补偿（自动为每个分片表启动一个 goroutine）
    _ = svc.StartAsyncTask(ctx)
}
```

## 配置选项

通过直接设置 `Service`/`ShardingService` 的字段来配置：

```go
svc.WaitDuration = time.Second * 30  // 消息超过多久未发送才触发补偿（默认 0，建议设置）
svc.MaxTimes = 3                     // 最大重试次数，超过后标记为 Fail（默认 3）
svc.BatchSize = 10                   // 每次补偿任务扫描的消息数量（默认 10）
```

### 使用批量发送模式

默认使用并行逐条发送（`CurMsgExecutor`），可以切换为批量发送：

```go
import "local_msg_tab/internal/service"

svc, _ := lmt.NewDefaultService(db, rdb, producer, logger,
    service.WithBatchExecutor(), // 使用批量发送
)
```

| 模式 | 说明 | 适用场景 |
|------|------|---------|
| CurMsgExecutor（默认） | 并行逐条发送，每条消息独立重试 | 消息量不大，需要精细控制 |
| BatchMsgExecutor | 批量调用 `SendMessages`，部分失败可识别 | 高吞吐场景 |

## 消息状态流转

```
Init (0) ──发送成功──> Success (1)
  |
  |──发送失败，未达上限──> Init (0)  [SendTimes+1, 等待下次补偿]
  |
  └──发送失败，达到上限──> Fail (2)  [终态]
```

## 完整示例

项目 `examples/` 目录下提供了完整的运行示例：

- [`examples/no_sharding/`](examples/no_sharding/main.go) — 非分库分表示例
- [`examples/sharding/`](examples/sharding/main.go) — 分库分表示例

## 注意事项

- **消费端幂等性**：本库保证 at-least-once 语义，消息可能重复发送，消费端必须保证幂等
- **分布式锁**：异步补偿任务使用 Redis 分布式锁，确保同一分片表同时只有一个实例在执行补偿
- **本地消息表与业务表同库**：本地消息表必须和业务表在同一个数据库中，才能利用数据库事务保证一致性
- **ExecTx 返回值**：即使 `ExecTx` 返回 error，业务事务可能已经提交成功（error 可能来自消息发送失败）
