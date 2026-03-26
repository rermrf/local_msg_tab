package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/domain"
	"local_msg_tab/internal/sharding"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/meoying/dlock-go"
	"github.com/rermrf/emo/logger"
	"github.com/rermrf/emo/slice"
	"gorm.io/gorm"
)

type ShardingService struct {
	DBs map[string]*gorm.DB
	// 返回 DB
	Sharding     sharding.Sharding
	producer     sarama.SyncProducer
	MaxTimes     int
	BatchSize    int
	WaitDuration time.Duration
	executor     Executor
	lockClient   dlock.Client
	logger       logger.Logger
}

func NewShardingService(
	dbs map[string]*gorm.DB,
	producer sarama.SyncProducer,
	lockClient dlock.Client,
	sharding sharding.Sharding,
	logger logger.Logger,
	opts ...ShardingServiceOpt,
) *ShardingService {
	svc := &ShardingService{
		DBs:        dbs,
		producer:   producer,
		MaxTimes:   3,
		BatchSize:  10,
		Sharding:   sharding,
		logger:     logger,
		lockClient: lockClient,
	}
	svc.executor = NewCurMsgExecutor(svc, logger)
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

type ShardingServiceOpt func(*ShardingService)

func WithBatchExecutor() ShardingServiceOpt {
	return func(svc *ShardingService) {
		svc.executor = NewBatchMsgExecutor(svc, svc.logger)
	}
}

func (s *ShardingService) StartAsyncTask(ctx context.Context) error {
	// 一个目标表一个 goroutine
	for _, dst := range s.Sharding.EffectiveTablesFunc() {
		task := AsyncTask{
			waitDuration: s.WaitDuration,
			executor:     s.executor,
			db:           s.DBs[dst.DB],
			dst:          dst,
			batchSize:    s.BatchSize,
			logger:       s.logger,
			lockClient:   s.lockClient,
		}
		go func() {
			task.Start(ctx)
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
		return fmt.Errorf("发送消息: %w, 原因 %w", syncSendMsgError, err)
	}
	return err
}

// ExecTx 我希望我作为一个业务方，我调用这个方法就 OK 了
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
	_, _, err = s.producer.SendMessage(newSaramaProducerMsg(msg))
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

func (s *ShardingService) sendMsgs(ctx context.Context, db *gorm.DB, table string, dmsgs []dao.LocalMsg) error {
	producerMsgs := make([]*sarama.ProducerMessage, 0, len(dmsgs))
	var topic string
	for _, dmsg := range dmsgs {
		var msg domain.Msg
		err := json.Unmarshal(dmsg.Data, &msg)
		if err != nil {
			return fmt.Errorf("提取消息内容失败: %w", err)
		}
		topic = msg.Topic
		producerMsgs = append(producerMsgs, newSaramaProducerMsgWithMetadata(dmsg.ID, msg))
	}
	// 发送消息
	err := s.producer.SendMessages(producerMsgs)

	failMsgs := make([]dao.LocalMsg, 0)
	initMsgs := make([]dao.LocalMsg, 0)
	successMsgs := make([]dao.LocalMsg, 0)
	failFields := map[string]interface{}{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusFail,
	}
	initFields := map[string]interface{}{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusInit,
	}
	successFields := map[string]interface{}{
		"utime":      time.Now().UnixMilli(),
		"send_times": gorm.Expr("send_times + 1"),
		"status":     dao.MsgStatusSuccess,
	}
	if err != nil {
		var errs sarama.ProducerErrors
		if errors.As(err, &errs) {
			var perr error
			failMsgs, initMsgs, successMsgs, perr = s.getPartitionMsgs(dmsgs, errs)
			if perr != nil {
				return perr
			}
		} else {
			failMsgs, initMsgs = s.getFailMsgs(dmsgs)
		}
	} else {
		successMsgs = dmsgs
	}
	if len(successMsgs) > 0 {
		err = s.updateMsgs(ctx, db, table, successMsgs, successFields, topic)
		if err != nil {
			return err
		}
	}
	if len(failMsgs) > 0 {
		// TODO 使用一个独立的 counter 来记录出现了补偿任务补发最终失败的情况
		s.logger.Error("发送消息失败", logger.String("topic", topic), logger.String("keys", s.getKeystr(failMsgs)))
		err = s.updateMsgs(ctx, db, table, failMsgs, failFields, topic)
		if err != nil {
			return err
		}
	}
	if len(initMsgs) > 0 {
		s.logger.Error("发送消息失败", logger.String("topic", topic), logger.String("keys", s.getKeystr(initMsgs)))
		err = s.updateMsgs(ctx, db, table, initMsgs, initFields, topic)
		if err != nil {
			return err
		}
	}
	return nil
}

// 部分失败获取消息
func (s *ShardingService) getPartitionMsgs(dmsgs []dao.LocalMsg, errMsgs sarama.ProducerErrors) ([]dao.LocalMsg, []dao.LocalMsg, []dao.LocalMsg, error) {
	failMsgs := make([]dao.LocalMsg, 0, len(errMsgs))
	initMsgs := make([]dao.LocalMsg, 0, len(errMsgs))
	successMsgs := make([]dao.LocalMsg, 0, len(dmsgs)-len(errMsgs))
	failMsgMap := make(map[int64]struct{}, len(errMsgs))
	for _, errMsg := range errMsgs {
		id, ok := errMsg.Msg.Metadata.(int64)
		if !ok {
			err := fmt.Errorf("生产者消息 metadata 类型不匹配 %T", errMsg.Msg.Metadata)
			return nil, nil, nil, fmt.Errorf("提取消息内容失败 %w", err)
		}
		failMsgMap[id] = struct{}{}
	}
	for _, dmsg := range dmsgs {
		if _, ok := failMsgMap[dmsg.ID]; ok {
			if dmsg.SendTimes+1 >= s.MaxTimes {
				failMsgs = append(failMsgs, dmsg)
			} else {
				initMsgs = append(initMsgs, dmsg)
			}
		} else {
			successMsgs = append(successMsgs, dmsg)
		}
	}
	return failMsgs, initMsgs, successMsgs, nil
}

// 获取完全失败的消息
func (s *ShardingService) getFailMsgs(dmsgs []dao.LocalMsg) ([]dao.LocalMsg, []dao.LocalMsg) {
	failMsgs := make([]dao.LocalMsg, 0, len(dmsgs))
	initMsgs := make([]dao.LocalMsg, 0, len(dmsgs))
	for _, dmsg := range dmsgs {
		if dmsg.SendTimes+1 >= s.MaxTimes {
			failMsgs = append(failMsgs, dmsg)
		} else {
			initMsgs = append(initMsgs, dmsg)
		}
	}
	return failMsgs, initMsgs
}

func (s *ShardingService) updateMsgs(ctx context.Context, db *gorm.DB, table string, dmsgs []dao.LocalMsg, fields map[string]interface{}, topic string) error {
	err := db.WithContext(ctx).Table(table).Where("id IN ?", s.getIDs(dmsgs)).Updates(fields).Error
	if err != nil {
		return fmt.Errorf("发送消息但是更新消息失败 %w, topic %s, keys %s",
			err, topic, fmt.Sprint(s.getIDs(dmsgs)))
	}
	return nil
}

func (s *ShardingService) getKeys(dmsgs []dao.LocalMsg) []string {
	return slice.Map(dmsgs, func(idx int, src dao.LocalMsg) string {
		return src.Key
	})
}

func (s *ShardingService) getIDs(dmsgs []dao.LocalMsg) []int64 {
	return slice.Map(dmsgs, func(idx int, src dao.LocalMsg) int64 {
		return src.ID
	})
}

func (s *ShardingService) getKeystr(msgs []dao.LocalMsg) string {
	keys := slice.Map(msgs, func(idx int, src dao.LocalMsg) string {
		return src.Key
	})
	return strings.Join(keys, ",")
}

func newSaramaProducerMsg(msg domain.Msg) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Key:       sarama.StringEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Content),
	}
}

func newSaramaProducerMsgWithMetadata(id int64, msg domain.Msg) *sarama.ProducerMessage {
	producerMsg := newSaramaProducerMsg(msg)
	producerMsg.Metadata = id
	return producerMsg
}
