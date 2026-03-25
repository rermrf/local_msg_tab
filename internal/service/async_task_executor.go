package service

import (
	"context"
	"fmt"
	"local_msg_tab/internal/dao"
	"time"

	"github.com/rermrf/emo/logger"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

type Executor interface {
	Exec(ctx context.Context, db *gorm.DB, table string) (int, error)
}

func finSuspendMsg(ctx context.Context, db *gorm.DB, waitDuration time.Duration, table string, offset, limit int) ([]dao.LocalMsg, error) {
	now := time.Now().UnixMilli()
	utime := now - waitDuration.Milliseconds()
	var res []dao.LocalMsg
	// 这里不需要检测重试次数，在发送消息的地方，当重试达到阈值，状态会被修改
	err := db.WithContext(ctx).Table(table).Where("status = ? AND utime < ?", dao.MsgStatusInit, utime).
		Offset(offset).Limit(limit).Find(&res).Error
	return res, err
}

// CurMsgExecutor 并行发送执行器
type CurMsgExecutor struct {
	svc    *ShardingService
	logger logger.Logger
}

func NewCurMsgExecutor(svc *ShardingService, logger logger.Logger) *CurMsgExecutor {
	return &CurMsgExecutor{svc: svc, logger: logger}
}

func (c *CurMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	data, err := finSuspendMsg(ctx, db, time.Second*30, table, 0, c.svc.BatchSize)
	if err != nil {
		c.logger.Error("查询数据失败", logger.String("err", err.Error()))
		return 0, fmt.Errorf("查询数据失败 %w", err)
	}
	c.logger.Debug("找到数据", logger.Int64("data", int64(len(data))))
	var eg errgroup.Group
	for _, msg := range data {
		shadow := msg
		eg.Go(func() error {
			err1 := c.svc.sendMsg(ctx, db, table, shadow)
			if err1 != nil {
				err1 = fmt.Errorf("发送消息失败 %w", err1)
			}
			return err1
		})
	}
	return len(data), eg.Wait()
}

// BatchMsgExecutor 批量发送消息执行器
type BatchMsgExecutor struct {
	svc    *ShardingService
	logger logger.Logger
}

func NewBatchMsgExecutor(svc *ShardingService, logger logger.Logger) *BatchMsgExecutor {
	return &BatchMsgExecutor{svc: svc, logger: logger}
}

func (b *BatchMsgExecutor) Exec(ctx context.Context, db *gorm.DB, table string) (int, error) {
	data, err := finSuspendMsg(ctx, db, time.Second*30, table, 0, b.svc.BatchSize)
	if err != nil {
		b.logger.Error("查询数据失败", logger.String("err", err.Error()))
		return 0, fmt.Errorf("查询数据失败 %w", err)
	}
	b.logger.Debug("找到数据", logger.Int64("data", int64(len(data))))
	err = b.svc.sendMsgs(ctx, db, table, data)
	if err != nil {
		return 0, fmt.Errorf("发送消息失败 %w", err)
	}
	return len(data), nil
}
