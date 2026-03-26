package sharding_order

import (
	"context"
	"fmt"
	"time"

	"github.com/bwmarrin/snowflake"
	"gorm.io/gorm"

	"local_msg_tab/internal/domain"
	"local_msg_tab/internal/service"
)

type OrderService struct {
	msg      *service.ShardingService
	snowNode *snowflake.Node
}

func NewOrderService(node *snowflake.Node, msgSvc *service.ShardingService) *OrderService {
	return &OrderService{msg: msgSvc, snowNode: node}
}

func (s *OrderService) CreateOrder(ctx context.Context, sn string, buyer int64) error {
	err := s.msg.ExecTx(ctx, buyer, func(tx *gorm.DB) (domain.Msg, error) {
		now := time.Now().UnixMilli()
		// 要按照分库分表的规则，推断出来表
		// 在使用分库分表的情况下，只要求本地消息表和订单表在同一个库
		// 不要求使用同样的分库分表规则
		tableName := fmt.Sprintf("orders_tab_%2d", (buyer%4)%2)
		err := tx.Table(tableName).Create(&Order{
			Id:    s.snowNode.Generate().Int64(),
			SN:    sn,
			Buyer: buyer,
			Utime: now,
			Ctime: now,
		}).Error
		if err != nil {
			return domain.Msg{}, err
		}
		return domain.Msg{
			Key:     sn,
			Topic:   "order_created",
			Content: sn,
		}, nil
	})
	return err
}

type Order struct {
	Id    int64 `gorm:"primaryKey,autoIncrement"`
	SN    string
	Buyer int64

	Utime int64
	Ctime int64
}
