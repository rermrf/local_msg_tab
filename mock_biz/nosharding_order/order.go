package nosharding_order

import (
	"context"
	"local_msg_tab/internal/domain"
	"local_msg_tab/internal/service"
	"time"

	"gorm.io/gorm"
)

// OrderService 模拟订单服务
type OrderService struct {
	db  *gorm.DB
	msg *service.Service
}

func NewOrderService(db *gorm.DB, msg *service.Service) *OrderService {
	return &OrderService{
		db:  db,
		msg: msg,
	}
}

// CreateOrder 最为典型的场景就是，在创建订单之后，要发送一个消息到消息队列上
func (s *OrderService) CreateOrder(ctx context.Context, sn string) error {
	err := s.msg.ExecTx(ctx, func(tx *gorm.DB) (domain.Msg, error) {
		now := time.Now().UnixMilli()
		o := &Order{
			SN:    sn,
			Utime: now,
			Ctime: now,
		}
		err := tx.Create(o).Error
		return domain.Msg{
			Key:     sn,
			Topic:   "order_created",
			Content: sn,
		}, err
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
