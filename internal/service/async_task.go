package service

import (
	"context"
	"errors"
	"fmt"
	"local_msg_tab/internal/sharding"
	"time"

	"github.com/meoying/dlock-go"
	"github.com/rermrf/emo/logger"
	"gorm.io/gorm"
)

type AsyncTask struct {
	waitDuration time.Duration
	dst          sharding.Dst
	executor     Executor
	db           *gorm.DB
	logger       logger.Logger
	battSize     int
	lockClient   dlock.Client
}

func (task *AsyncTask) Start(ctx context.Context) {
	key := fmt.Sprintf("%s:%s", task.dst.DB, task.dst.Table)
	task.logger.With(logger.String("key", key))
	interval := time.Minute
	for {
		lock, err := task.lockClient.NewLock(ctx, key, interval)
		if err != nil {
			task.logger.Error("初始化分布式锁失败，重试", logger.Error(err))
			// 暂停一会
			time.Sleep(interval)
			continue
		}

		lockCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		// 没有拿到锁，不管是系统错误，还是锁被人持有，都没有关系
		// 暂停一段时间之后继续
		err = lock.Lock(lockCtx)
		cancel()

		if err != nil {
			if errors.Is(err, dlock.ErrLocked) {
				task.logger.Info("没有抢到分布式锁，此时正有人持有锁")
			} else {
				task.logger.Error("没有抢到分布式锁，系统出现问题", logger.Error(err))
			}
			time.Sleep(interval)
			continue
		}

		// 开启任务循环
		task.refreshAndLoop(ctx, lock)
		// 只要这个方法返回，就要释放分布式锁
		if unErr := lock.Unlock(ctx); unErr != nil {
			task.logger.Error("释放分布式失败", logger.Error(unErr))
		}
		// 从这里退出的时候，要检测一下是不是要结束了
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			task.logger.Info("任务被取消，退出任务循环")
			return
		default:
			task.logger.Error("执行补偿任务失败，将执行重试")
			time.Sleep(interval)
		}
	}
}

func (task *AsyncTask) refreshAndLoop(ctx context.Context, lock dlock.Lock) {
	// 这里有两个动作：
	// 1. 执行异步补偿任务
	// 2. 判定要不要让出分布式锁
	// 3. 即当自身执行出错比较高的时候，就让出分布式锁

	errCnt := 0
	for {
		// 刷新的过期时间应该很短
		refreshCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		// 手动控制每一批次循环开始就续约分布式锁
		// 而后控制每一批次的超时时间
		// 这样就可以确保不会出现分布式锁过期，但是任务还在运行的情况
		err := lock.Refresh(refreshCtx)
		cancel()
		if err != nil {
			task.logger.Error("分布式锁续约失败，直接返回", logger.Error(err))
			return
		}

		cnt, err := task.loop(ctx)
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded), errors.Is(ctxErr, context.Canceled):
			// 最上层用户取消
			return
		case err != nil:
			// 执行出错
			errCnt++
			const threshold = 5
			if errCnt >= threshold {
				task.logger.Error("执行任务连续出错，退出循环", logger.Error(err))
				return
			}
		default:
			// 重置
			errCnt = 0
			if cnt == 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

func (task *AsyncTask) loop(ctx context.Context) (int, error) {
	loopCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	return task.executor.Exec(loopCtx, task.db, task.dst.Table)
}
