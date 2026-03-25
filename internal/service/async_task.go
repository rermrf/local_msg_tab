package service

import (
	"context"
	"fmt"
	"local_msg_tab/internal/sharding"
	"time"
)

type AsyncTask struct {
	wait time.Duration
	dst  sharding.Dst
}

func (task *AsyncTask) Start(ctx context.Context) {
	key := fmt.Sprintf("%s:%s", task.dst.DB, task.dst.Table)

}
