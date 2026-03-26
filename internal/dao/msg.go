package dao

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type MsgDao struct {
	db *gorm.DB
	// 最大初始化时间，也就是说超过这个，我们就认为消息是发送失败了
	// 补偿任务就会启动补偿
	duration time.Duration
}

func NewMsgDao(db *gorm.DB) *MsgDao {
	return &MsgDao{
		db: db,
	}
}

func (dao *MsgDao) Get(ctx context.Context, table string, id int64) (LocalMsg, error) {
	var result LocalMsg
	err := dao.db.WithContext(ctx).Table(table).Where("id = ?", id).First(&result).Error
	return result, err
}

func (dao *MsgDao) List(ctx context.Context, q Query) ([]LocalMsg, error) {
	var res []LocalMsg
	db := dao.db.WithContext(ctx).
		Offset(q.Offset).
		Limit(q.Limit).
		Table(q.Table).Order("id DESC")
	if q.Status >= 0 {
		db = db.Where("status=?", q.Status)
	}
	if q.Key != "" {
		db = db.Where("`key` = ?", q.Key)
	}

	if q.StartTime > 0 {
		db = db.Where("`ctime` >= ?", q.StartTime)
	}

	if q.EndTime > 0 {
		db = db.Where("`ctime` <= ?", q.EndTime)
	}

	err := db.Find(&res).Error
	return res, err
}

type Query struct {
	Table  string `json:"table,omitempty"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Status int8   `json:"status,omitempty"`
	// Key 是精准查询
	Key string `json:"key,omitempty"`

	StartTime int64 `json:"startTime,omitempty"`
	EndTime   int64 `json:"endTime,omitempty"`
}

type LocalMsg struct {
	ID        int64  `json:"id" gorm:"primaryKey,autoIncrement"`
	Key       string `json:"key" gorm:"index"`
	Data      []byte `json:"data" gorm:"type:text"`
	Status    int8   `json:"status"`
	SendTimes int
	Ctime     int64
	Utime     int64
}

const (
	MsgStatusInit int8 = iota
	MsgStatusSuccess
	MsgStatusFail
)
