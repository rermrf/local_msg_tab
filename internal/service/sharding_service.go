package service

import (
	"context"
	"encoding/json"
	"fmt"
	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/domain"
	"local_msg_tab/internal/sharding"
	"time"

	"github.com/IBM/sarama"
	"github.com/rermrf/emo/logger"
	"gorm.io/gorm"
)

type ShardingService struct {
	DBs map[string]*gorm.DB
	// 返回 DB
	Sharding  sharding.Sharding
	producer  sarama.SyncProducer
	MaxTimes  int
	BatchSize int
	logger    logger.Logger
}

func NewShardingService(dbs map[string]*gorm.DB, producer sarama.SyncProducer) *ShardingService {
	return &ShardingService{}
}

func (s *ShardingService) StartAsyncTask() error {
	const limit = 10
	// 一个目标表一个 goroutine
	for _, dst := range s.Sharding.EffevtiveTablesFunc() {
		dst := dst
		go func() {
			db, ok := s.DBs[dst.DB]
			if !ok {
				// 打点日志
				return
			}
			for {
				// time.Minute.Milliseconds() 根据对同步发送消息的时间估计
				now := time.Now().UnixMilli() - 3*time.Second.Milliseconds()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				var msgs []dao.LocalMsg
				err := db.WithContext(ctx).Table(dst.Table).Where("status = ? AND utime < ?", dao.MsgStatusInit, now).
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

func (s *ShardingService) execTx(ctx context.Context, db *gorm.DB, table string, biz func(tx *gorm.DB) (domain.Msg, error)) error {
	var dmsg dao.LocalMsg
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err := biz(tx)
		if err != nil {
			// 代表的是业务错误
			return err
		}
		dmsg = s.newDmsg(msg)
		// 在补偿任务 1:1 模型之下，这边可以使用自增主键
		dmsg.Ctime = time.Now().UnixMilli()
		dmsg.Utime = time.Now().UnixMilli()
		return tx.Table(table).Create(&dmsg).Error
	})
	if err != nil {
		return err
	}
	err = s.sendMsg(ctx, db, table, dmsg)
	if err != nil {
		// 站在业务方的角度来讲，它没有失败，他的业务已经在上面那个事务中已经执行成功
		// 打印日志，而后不用管
		s.logger.Error("发送消息失败", logger.Any("error", err))
		return fmt.Errorf("send msg: %w, cause %w", syncSendMsgError, err)
	}
	return err
}

// 我希望我作为一个业务方，我调用这个方法就 OK 了
func (s *ShardingService) ExecTx(ctx context.Context, shardingKey any, biz func(tx *gorm.DB) (domain.Msg, error)) error {
	dst := s.Sharding.ShardingFunc(shardingKey)
	db, ok := s.DBs[dst.DB]
	if !ok {
		// 没有这个 DB
		// 1. shardingFunc 有问题
		// 2. 你的参数有问题
		// 3. 你初始化的时候有问题
		return fmt.Errorf("db %s not exist", dst.DB)
	}
	return s.execTx(ctx, db, dst.Table, biz)
}

func (s *ShardingService) sendMsg(ctx context.Context, db *gorm.DB, table string, dmsg dao.LocalMsg) error {
	// dao层的msg中提取需要发送的msg
	var msg domain.Msg
	err := json.Unmarshal(dmsg.Data, &msg)
	if err != nil {
		return fmt.Errorf("unmarshal msg: %w", err)
	}
	// 发送消息
	_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	times := dmsg.SendTimes + 1
	var updates = map[string]interface{}{
		"utime":      time.Now().UnixMilli(),
		"send_times": times,
		"status":     dao.MsgStatusInit,
	}
	if err != nil {
		s.logger.Error("发送消息失败",
			logger.String("Topic", msg.Topic),
			logger.String("Key", msg.Key),
			logger.Int32("send_times", int32(times)),
		)
		if times >= s.MaxTimes {
			updates["status"] = dao.MsgStatusFail
		}
	} else {
		updates["status"] = dao.MsgStatusSuccess
	}

	err1 := db.WithContext(ctx).Table(table).Where("id = ?", dmsg.ID).Updates(updates).Error
	if err1 != nil {
		return fmt.Errorf("发送消息但是更新消息失败: %w, 发送结果: %w, topic: %s, key: %s", err1, err, msg.Topic, msg.Key)
	}
	return err
}

func (s *ShardingService) newDmsg(msg domain.Msg) dao.LocalMsg {
	val, _ := json.Marshal(msg)
	now := time.Now().UnixMilli()
	return dao.LocalMsg{
		Data:      val,
		Status:    dao.MsgStatusInit,
		Key:       msg.Key,
		SendTimes: 0,
		Utime:     now,
		Ctime:     now,
	}
}

func (s *ShardingService) sendMsgs(ctx context.Context, db *gorm.DB, table string, msgs []dao.LocalMsg) error {
	msgs := make([]dao.LocalMsg, len(data))
	var topic string
	for _, msg := range msgs {

	}
}
