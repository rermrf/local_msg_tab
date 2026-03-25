package service

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"local_msg_tab/internal/sharding"
	"time"
)

type ShardingService struct {
	dbs map[string]*gorm.DB
	// 返回 DB
	shardingFunc func(shardingKey any) sharding.Dst
	allTables    []sharding.Dst
	producer     sarama.SyncProducer
}

func NewShardingService(dbs map[string]*gorm.DB, producer sarama.SyncProducer) *ShardingService {}

func (s *ShardingService) StartAsyncTask() error {
	const limit = 10
	// 一个目标表一个 goroutine
	for _, dst := range s.allTables {
		dst := dst
		go func() {
			db, ok := s.dbs[dst.DB]
			if !ok {
				// 打点日志
				return
			}
			for {
				// time.Minute.Milliseconds() 根据对同步发送消息的时间估计
				now := time.Now().UnixMilli() - 3*time.Second.Milliseconds()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				var msgs []Msg
				err := db.WithContext(ctx).Table(dst.Table).Where("status = ? AND utime < ?", MsgStatusInit, now).
					Offset(0).Limit(limit).Find(&msgs).Error
				cancel()
				if err != nil {
					// 要不要结束这个异步任务
					// 可以考虑进一步区分是偶发性错误，还是持续性错误
					continue
				}
				for _, msg := range msgs {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					err = s.sendMsg(ctx, db, dst.Table, msg)
					cancel()
					if err != nil {
						// 怎么处理？
						msg.SendTimes++
						continue
					}
				}
				if len(msgs) == 0 {
					time.Sleep(1 * time.Second)
				}

			}
		}()
	}
	return nil
}

// 我希望我作为一个业务方，我调用这个方法就 OK 了
func (s *ShardingService) ExecTx(ctx context.Context, shardingKey any, biz func(tx *gorm.DB) (Msg, error)) error {
	dst := s.shardingFunc(shardingKey)
	db, ok := s.dbs[dst.DB]
	if !ok {
		// 没有这个 DB
		// 1. shardingFunc 有问题
		// 2. 你的参数有问题
		// 3. 你初始化的时候有问题
		return fmt.Errorf("db %s not exist", dst.DB)
	}
	var msg Msg
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err = biz(tx)
		if err != nil {
			// 代表的是业务错误
			return err
		}
		// 在补偿任务 1:1 模型之下，这边可以使用自增主键
		msg.Ctime = time.Now().UnixMilli()
		msg.Utime = time.Now().UnixMilli()
		return tx.Table(dst.Table).Create(&msg).Error
	})
	if err != nil {
		return err
	}
	err = s.sendMsg(ctx, db, dst.Table, msg)
	if err != nil {
		// 站在业务方的角度来讲，它没有失败，他的业务已经在上面那个事务中已经执行成功
		// 打印日志，而后不用管
		return fmt.Errorf("send msg: %w, cause %w", syncSendMsgError, err)
	}
	return nil
}

func (s *ShardingService) sendMsg(ctx context.Context, db *gorm.DB, table string, msg Msg) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Table(table).Where("id = ?", msg.ID).Updates(map[string]interface{}{
		"status": MsgStatusSuccess,
		"utime":  time.Now().UnixMilli(),
	}).Error
}
