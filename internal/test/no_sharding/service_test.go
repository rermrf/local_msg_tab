package no_sharding

import (
	"bytes"
	"context"
	"errors"
	"local_msg_tab"
	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/test"
	"local_msg_tab/internal/test/mocks"
	"local_msg_tab/mock_biz/nosharding_order"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/rermrf/emo/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type OrderServiceTestSuite struct {
	rdb    redis.Cmdable
	db     *gorm.DB
	logger logger.Logger
	test.BaseSuite
}

func TestService(t *testing.T) {
	suite.Run(t, new(OrderServiceTestSuite))
}

func (s *OrderServiceTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:3306)/test?charset=utf8&parseTime=True&loc=Local"))
	require.NoError(s.T(), err)
	err = db.AutoMigrate(&dao.LocalMsg{}, &nosharding_order.Order{})
	require.NoError(s.T(), err)
	s.db = db
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	s.rdb = rdb
	l := logger.NewNopLogger()
	s.logger = l
}

func (s *OrderServiceTestSuite) TearDownSuite() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db.WithContext(ctx).Exec("TRUNCATE TABLE orders;").Error
	assert.NoError(s.T(), err)
	err = s.db.WithContext(ctx).Exec("TRUNCATE TABLE local_msgs;").Error
	assert.NoError(s.T(), err)
}

func (s *OrderServiceTestSuite) TearDownTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db.WithContext(ctx).Exec("TRUNCATE TABLE local_msgs;").Error
	assert.NoError(s.T(), err)
}

func (s *OrderServiceTestSuite) getMsg(id int64) dao.LocalMsg {
	var msg dao.LocalMsg
	err := s.db.Table("local_msgs").Where("id = ?", id).First(&msg).Error
	require.NoError(s.T(), err)
	return msg
}

func (s *OrderServiceTestSuite) TestAsyncTask() {
	msgs := make([]dao.LocalMsg, 0)
	now := time.Now().UnixMilli()
	msg1 := s.MockDAOMsg(1, now-(time.Second*11).Milliseconds())
	msgs = append(msgs, msg1)

	msg2 := s.MockDAOMsg(2, now-(time.Second*11).Milliseconds())
	msg2.Status = dao.MsgStatusFail
	msgs = append(msgs, msg2)

	msg3 := s.MockDAOMsg(3, now-(time.Second*11).Milliseconds())
	msg3.Status = dao.MsgStatusSuccess
	msgs = append(msgs, msg3)

	msg4 := s.MockDAOMsg(4, now-(time.Second*1).Milliseconds())
	msg4.Status = dao.MsgStatusInit
	msgs = append(msgs, msg4)

	msg5 := s.MockDAOMsg(5, now-(time.Second*13).Milliseconds())
	msg5.Status = dao.MsgStatusInit
	msg5.SendTimes = 2
	msgs = append(msgs, msg5)

	msg6 := s.MockDAOMsg(6, now-(time.Second*13).Milliseconds())
	msg6.Status = dao.MsgStatusInit
	msg6.SendTimes = 2
	msgs = append(msgs, msg6)

	err := s.db.Create(&msgs).Error
	require.NoError(s.T(), err)
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	producer := mocks.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).DoAndReturn(func(message *sarama.ProducerMessage) (int32, int64, error) {
		data := []byte(message.Key.(sarama.StringEncoder))
		if bytes.Contains(data, []byte("success")) {
			return 1, 1, nil
		}
		return 0, 0, errors.New("mock error")
	}).AnyTimes()

	svc, err := local_msg_tab.NewDefaultService(s.db, s.rdb, producer, s.logger)
	require.NoError(s.T(), err)
	svc.WaitDuration = time.Second * 10
	svc.MaxTimes = 3
	svc.BatchSize = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = svc.StartAsyncTask(ctx)
	require.NoError(s.T(), err)

	// 等待异步补偿任务处理完成
	// msg1 是符合条件的消息，轮询它的状态变化来判断补偿任务是否已执行
	assert.Eventually(s.T(), func() bool {
		msg := s.getMsg(1)
		return msg.Status == dao.MsgStatusSuccess
	}, 30*time.Second, 500*time.Millisecond, "等待 msg1 变为 Success 超时")

	// 等待所有批次处理完成（BatchSize=2，需要多轮）
	assert.Eventually(s.T(), func() bool {
		msg := s.getMsg(6)
		return msg.Status == dao.MsgStatusFail
	}, 30*time.Second, 500*time.Millisecond, "等待 msg6 变为 Fail 超时")

	cancel()

	// 验证每条消息的最终状态
	// msg1: Init + 发送成功 → Success, SendTimes=1
	actual1 := s.getMsg(1)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual1.Status)
	assert.Equal(s.T(), 1, actual1.SendTimes)

	// msg2: 初始就是 Fail 状态，补偿任务不会处理 → 保持不变
	actual2 := s.getMsg(2)
	assert.Equal(s.T(), dao.MsgStatusFail, actual2.Status)
	assert.Equal(s.T(), 0, actual2.SendTimes)

	// msg3: 初始就是 Success 状态，补偿任务不会处理 → 保持不变
	actual3 := s.getMsg(3)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual3.Status)
	assert.Equal(s.T(), 0, actual3.SendTimes)

	// msg4: Init 但 utime 太近（1s前），未超过 WaitDuration(10s) → 保持不变
	actual4 := s.getMsg(4)
	assert.Equal(s.T(), dao.MsgStatusInit, actual4.Status)
	assert.Equal(s.T(), 0, actual4.SendTimes)

	// msg5: Init + SendTimes=2 + 发送成功 → Success, SendTimes=3
	actual5 := s.getMsg(5)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual5.Status)
	assert.Equal(s.T(), 3, actual5.SendTimes)

	// msg6: Init + SendTimes=2 + 发送失败 + 达到 MaxTimes(3) → Fail, SendTimes=3
	actual6 := s.getMsg(6)
	assert.Equal(s.T(), dao.MsgStatusFail, actual6.Status)
	assert.Equal(s.T(), 3, actual6.SendTimes)
}
