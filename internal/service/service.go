package service

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"time"
)

var syncSendMsgError = fmt.Errorf("sync send message error")

type Service struct {
	db       *gorm.DB
	producer sarama.SyncProducer
}

func (s *Service) StartAsyncTask() error {
	const limit = 10
	for {
		// time.Minute.Milliseconds() 根据对同步发送消息的时间估计
		now := time.Now().UnixMilli() - 3*time.Second.Milliseconds()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var msgs []Msg
		err := s.db.WithContext(ctx).Where("status = ? AND utime < ?", MsgStatusInit, now).
			Offset(0).Limit(limit).Find(&msgs).Error
		cancel()
		if err != nil {
			// 要不要结束这个异步任务
			// 可以考虑进一步区分是偶发性错误，还是持续性错误
			continue
		}
		for _, msg := range msgs {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = s.sendMsg(ctx, msg)
			cancel()
			if err != nil {
				// 怎么处理？
				msg.SendTimes++
				continue
			}
		}

	}
}

// 我希望我作为一个业务方，我调用这个方法就 OK 了
func (s *Service) ExecTx(ctx context.Context, biz func(tx *gorm.DB) (Msg, error)) error {
	var msg Msg
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err = biz(tx)
		if err != nil {
			// 代表的是业务错误
			return err
		}
		msg.Ctime = time.Now().UnixMilli()
		msg.Utime = time.Now().UnixMilli()
		return tx.Create(&msg).Error
	})
	if err != nil {
		return err
	}
	err = s.sendMsg(ctx, msg)
	if err != nil {
		// 站在业务方的角度来讲，它没有失败，他的业务已经在上面那个事务中已经执行成功
		// 打印日志，而后不用管
		return fmt.Errorf("send msg: %w, cause %w", syncSendMsgError, err)
	}
	return nil
}

func (s *Service) sendMsg(ctx context.Context, msg Msg) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Where("id = ?", msg.ID).Updates(map[string]interface{}{
		"status": MsgStatusSuccess,
		"utime":  time.Now().UnixMilli(),
	}).Error
}

type Msg struct {
	ID        uint64 `json:"id"`
	Topic     string
	Content   string
	status    MsgStatus
	SendTimes int64
	Ctime     int64
	Utime     int64
}

type MsgStatus string

const (
	MsgStatusInit    MsgStatus = "init"
	MsgStatusSuccess MsgStatus = "SUCCESS"
)
