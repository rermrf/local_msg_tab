package dao

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
