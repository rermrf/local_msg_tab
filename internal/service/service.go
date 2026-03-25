package service

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"local_msg_tab/internal/domain"
)

var syncSendMsgError = fmt.Errorf("sync send message error")

type Service struct {
	*ShardingService
}

func (s *Service) ExecTx(ctx context.Context, shardingKey any, biz func(tx *gorm.DB) (domain.Msg, error)) error {
	db := s.DBs[""]
	return s.execTx(ctx, db, "", biz)
}
