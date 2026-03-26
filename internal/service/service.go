package service

import (
	"context"
	"fmt"
	"local_msg_tab/internal/domain"

	"gorm.io/gorm"
)

var syncSendMsgError = fmt.Errorf("同步发送消息失败")

type Service struct {
	*ShardingService
}

func (s *Service) ExecTx(ctx context.Context, biz func(tx *gorm.DB) (domain.Msg, error)) error {
	db := s.DBs[""]
	return s.execTx(ctx, db, "", biz)
}
