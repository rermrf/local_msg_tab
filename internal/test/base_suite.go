package test

import (
	"encoding/json"
	"fmt"
	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BaseSuite struct {
	suite.Suite
}

func (s *BaseSuite) AssertMsg(expected, actual dao.LocalMsg) {
	expected.Utime = 0
	expected.Ctime = 0
	actual.Utime = 0
	actual.Ctime = 0
	assert.Equal(s.T(), expected, actual)
}

func (s *BaseSuite) MockDAOMsg(id int64, utime int64) dao.LocalMsg {
	var key string
	if id%2 == 1 {
		key = fmt.Sprintf("%d_success", id)
	} else {
		key = fmt.Sprintf("%d_fail", id)
	}

	msg := domain.Msg{
		Key:     key,
		Topic:   "order_created",
		Content: "这是内容",
	}
	val, _ := json.Marshal(msg)
	return dao.LocalMsg{
		ID:     id,
		Key:    key,
		Data:   val,
		Status: dao.MsgStatusInit,
		Utime:  utime,
		Ctime:  utime,
	}
}
