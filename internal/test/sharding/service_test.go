package sharding

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"local_msg_tab"
	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/service"
	"local_msg_tab/internal/sharding"
	"local_msg_tab/internal/test"
	"local_msg_tab/internal/test/mocks"
	"testing"
	"time"

	"github.com/IBM/sarama"
	dredis "github.com/meoying/dlock-go/redis"
	"github.com/redis/go-redis/v9"
	"github.com/rermrf/emo/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 分片策略：2 库 2 表
// shardingKey % 2 → 选库 (test_shard_0 / test_shard_1)
// shardingKey / 2 % 2 → 选表 (local_msgs_0 / local_msgs_1)

var (
	dbNames    = []string{"test_shard_0", "test_shard_1"}
	tableNames = []string{"local_msgs_0", "local_msgs_1"}
)

func newSharding() sharding.Sharding {
	return sharding.Sharding{
		ShardingFunc: func(key any) sharding.Dst {
			k := key.(int64)
			dbIdx := k % 2
			tableIdx := (k / 2) % 2
			return sharding.Dst{
				DB:    dbNames[dbIdx],
				Table: tableNames[tableIdx],
			}
		},
		EffectiveTablesFunc: func() []sharding.Dst {
			dsts := make([]sharding.Dst, 0, 4)
			for _, db := range dbNames {
				for _, table := range tableNames {
					dsts = append(dsts, sharding.Dst{DB: db, Table: table})
				}
			}
			return dsts
		},
	}
}

type ShardingServiceTestSuite struct {
	dbs    map[string]*gorm.DB
	rdb    redis.Cmdable
	logger logger.Logger
	// rootDB 用于创建/删除 database
	rootDB *gorm.DB
	test.BaseSuite
}

func TestShardingService(t *testing.T) {
	suite.Run(t, new(ShardingServiceTestSuite))
}

func (s *ShardingServiceTestSuite) SetupSuite() {
	// 先用不指定 database 的连接创建两个 database
	rootDB, err := gorm.Open(mysql.Open("root:root@tcp(localhost:3306)/?charset=utf8&parseTime=True&loc=Local"))
	require.NoError(s.T(), err)
	s.rootDB = rootDB

	for _, dbName := range dbNames {
		err = rootDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)).Error
		require.NoError(s.T(), err)
	}

	// 为每个 database 创建连接并建表
	s.dbs = make(map[string]*gorm.DB, len(dbNames))
	for _, dbName := range dbNames {
		db, err := gorm.Open(mysql.Open(
			fmt.Sprintf("root:root@tcp(localhost:3306)/%s?charset=utf8&parseTime=True&loc=Local", dbName),
		))
		require.NoError(s.T(), err)
		s.dbs[dbName] = db

		for _, tableName := range tableNames {
			err = db.Table(tableName).AutoMigrate(&dao.LocalMsg{})
			require.NoError(s.T(), err)
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	s.rdb = rdb
	s.logger = logger.NewNopLogger()
}

func (s *ShardingServiceTestSuite) TearDownSuite() {
	for _, dbName := range dbNames {
		err := s.rootDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)).Error
		assert.NoError(s.T(), err)
	}
}

func (s *ShardingServiceTestSuite) TearDownTest() {
	for _, dbName := range dbNames {
		db := s.dbs[dbName]
		for _, tableName := range tableNames {
			err := db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)).Error
			assert.NoError(s.T(), err)
		}
	}
}

func (s *ShardingServiceTestSuite) getMsg(dbName, tableName string, id int64) dao.LocalMsg {
	var msg dao.LocalMsg
	err := s.dbs[dbName].Table(tableName).Where("id = ?", id).First(&msg).Error
	require.NoError(s.T(), err)
	return msg
}

func (s *ShardingServiceTestSuite) insertMsg(dbName, tableName string, msg dao.LocalMsg) {
	err := s.dbs[dbName].Table(tableName).Create(&msg).Error
	require.NoError(s.T(), err)
}

func (s *ShardingServiceTestSuite) TestAsyncTask() {
	now := time.Now().UnixMilli()
	oldUtime := now - (time.Second * 11).Milliseconds()
	recentUtime := now - (time.Second * 1).Milliseconds()
	veryOldUtime := now - (time.Second * 13).Milliseconds()

	// 分片规则:
	// shardingKey % 2 → DB, shardingKey / 2 % 2 → Table
	// key=0 → db0, table0
	// key=1 → db1, table0
	// key=2 → db0, table1
	// key=3 → db1, table1

	// 在每个分片表中各插入代表性的测试数据
	// 分片 db0/table0: 插入一条可补偿成功的消息和一条已失败的消息
	msg1 := s.MockDAOMsg(1, oldUtime) // key=1_success → 发送成功
	s.insertMsg(dbNames[0], tableNames[0], msg1)

	msg2 := s.MockDAOMsg(2, oldUtime) // key=2_fail
	msg2.Status = dao.MsgStatusFail   // 已是终态，不处理
	s.insertMsg(dbNames[0], tableNames[0], msg2)

	// 分片 db1/table0: 插入一条已成功的消息和一条 utime 太近的消息
	msg3 := s.MockDAOMsg(3, oldUtime)    // key=3_success
	msg3.Status = dao.MsgStatusSuccess   // 已是终态，不处理
	s.insertMsg(dbNames[1], tableNames[0], msg3)

	msg4 := s.MockDAOMsg(4, recentUtime) // key=4_fail, utime 太近，不处理
	s.insertMsg(dbNames[1], tableNames[0], msg4)

	// 分片 db0/table1: 插入一条达到重试上限且能成功的消息
	msg5 := s.MockDAOMsg(5, veryOldUtime) // key=5_success → 发送成功
	msg5.SendTimes = 2
	s.insertMsg(dbNames[0], tableNames[1], msg5)

	// 分片 db1/table1: 插入一条达到重试上限且会失败的消息
	msg6 := s.MockDAOMsg(6, veryOldUtime) // key=6_fail → 发送失败，达到 MaxTimes → Fail
	msg6.SendTimes = 2
	s.insertMsg(dbNames[1], tableNames[1], msg6)

	// 设置 mock producer
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

	// 创建分库分表 service
	lockClient := dredis.NewClient(s.rdb)
	svc := local_msg_tab.NewDefaultShardingService(
		s.dbs, producer, lockClient, newSharding(), s.logger,
	)
	svc.WaitDuration = time.Second * 10
	svc.MaxTimes = 3
	svc.BatchSize = 10

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := svc.StartAsyncTask(ctx)
	require.NoError(s.T(), err)

	// 等待补偿任务处理 msg1 (db0/table0)
	assert.Eventually(s.T(), func() bool {
		msg := s.getMsg(dbNames[0], tableNames[0], 1)
		return msg.Status == dao.MsgStatusSuccess
	}, 30*time.Second, 500*time.Millisecond, "等待 msg1 变为 Success 超时")

	// 等待补偿任务处理 msg6 (db1/table1)
	assert.Eventually(s.T(), func() bool {
		msg := s.getMsg(dbNames[1], tableNames[1], 6)
		return msg.Status == dao.MsgStatusFail
	}, 30*time.Second, 500*time.Millisecond, "等待 msg6 变为 Fail 超时")

	// 等待 msg5 (db0/table1)
	assert.Eventually(s.T(), func() bool {
		msg := s.getMsg(dbNames[0], tableNames[1], 5)
		return msg.Status == dao.MsgStatusSuccess
	}, 30*time.Second, 500*time.Millisecond, "等待 msg5 变为 Success 超时")

	cancel()

	// === 验证每条消息的最终状态 ===

	// msg1: Init + 发送成功 → Success, SendTimes=1
	actual1 := s.getMsg(dbNames[0], tableNames[0], 1)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual1.Status)
	assert.Equal(s.T(), 1, actual1.SendTimes)

	// msg2: 初始就是 Fail，不会被补偿 → 保持不变
	actual2 := s.getMsg(dbNames[0], tableNames[0], 2)
	assert.Equal(s.T(), dao.MsgStatusFail, actual2.Status)
	assert.Equal(s.T(), 0, actual2.SendTimes)

	// msg3: 初始就是 Success，不会被补偿 → 保持不变
	actual3 := s.getMsg(dbNames[1], tableNames[0], 3)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual3.Status)
	assert.Equal(s.T(), 0, actual3.SendTimes)

	// msg4: Init 但 utime 太近（1s前），未超过 WaitDuration(10s) → 保持不变
	actual4 := s.getMsg(dbNames[1], tableNames[0], 4)
	assert.Equal(s.T(), dao.MsgStatusInit, actual4.Status)
	assert.Equal(s.T(), 0, actual4.SendTimes)

	// msg5: Init + SendTimes=2 + 发送成功 → Success, SendTimes=3
	actual5 := s.getMsg(dbNames[0], tableNames[1], 5)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual5.Status)
	assert.Equal(s.T(), 3, actual5.SendTimes)

	// msg6: Init + SendTimes=2 + 发送失败 + 达到 MaxTimes(3) → Fail, SendTimes=3
	actual6 := s.getMsg(dbNames[1], tableNames[1], 6)
	assert.Equal(s.T(), dao.MsgStatusFail, actual6.Status)
	assert.Equal(s.T(), 3, actual6.SendTimes)
}

// TestExecTx 测试分库分表场景下的事务执行
func (s *ShardingServiceTestSuite) TestExecTx() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	producer := mocks.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(1), nil).AnyTimes()

	lockClient := dredis.NewClient(s.rdb)
	svc := local_msg_tab.NewDefaultShardingService(
		s.dbs, producer, lockClient, newSharding(), s.logger,
	)
	svc.MaxTimes = 3

	ctx := context.Background()

	// shardingKey=0 → db0/table0
	err := svc.ExecTx(ctx, int64(0), func(tx *gorm.DB) (local_msg_tab.Msg, error) {
		return local_msg_tab.Msg{
			Key:     "order_0",
			Topic:   "order_created",
			Content: "内容0",
		}, nil
	})
	require.NoError(s.T(), err)

	// shardingKey=1 → db1/table0
	err = svc.ExecTx(ctx, int64(1), func(tx *gorm.DB) (local_msg_tab.Msg, error) {
		return local_msg_tab.Msg{
			Key:     "order_1",
			Topic:   "order_created",
			Content: "内容1",
		}, nil
	})
	require.NoError(s.T(), err)

	// shardingKey=2 → db0/table1
	err = svc.ExecTx(ctx, int64(2), func(tx *gorm.DB) (local_msg_tab.Msg, error) {
		return local_msg_tab.Msg{
			Key:     "order_2",
			Topic:   "order_created",
			Content: "内容2",
		}, nil
	})
	require.NoError(s.T(), err)

	// shardingKey=3 → db1/table1
	err = svc.ExecTx(ctx, int64(3), func(tx *gorm.DB) (local_msg_tab.Msg, error) {
		return local_msg_tab.Msg{
			Key:     "order_3",
			Topic:   "order_created",
			Content: "内容3",
		}, nil
	})
	require.NoError(s.T(), err)

	// 验证消息写入了正确的分片
	actual0 := s.getMsg(dbNames[0], tableNames[0], 1)
	assert.Equal(s.T(), "order_0", actual0.Key)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual0.Status)

	actual1 := s.getMsg(dbNames[1], tableNames[0], 1)
	assert.Equal(s.T(), "order_1", actual1.Key)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual1.Status)

	actual2 := s.getMsg(dbNames[0], tableNames[1], 1)
	assert.Equal(s.T(), "order_2", actual2.Key)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual2.Status)

	actual3 := s.getMsg(dbNames[1], tableNames[1], 1)
	assert.Equal(s.T(), "order_3", actual3.Key)
	assert.Equal(s.T(), dao.MsgStatusSuccess, actual3.Status)
}

// TestExecTxWithInvalidDB 测试使用不存在的分库
func (s *ShardingServiceTestSuite) TestExecTxWithInvalidDB() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	producer := mocks.NewMockSyncProducer(ctrl)

	lockClient := dredis.NewClient(s.rdb)
	badSharding := sharding.Sharding{
		ShardingFunc: func(key any) sharding.Dst {
			return sharding.Dst{DB: "not_exist_db", Table: "local_msgs_0"}
		},
		EffectiveTablesFunc: func() []sharding.Dst {
			return nil
		},
	}
	svc := service.NewShardingService(s.dbs, producer, lockClient, badSharding, s.logger)

	err := svc.ExecTx(context.Background(), int64(0), func(tx *gorm.DB) (local_msg_tab.Msg, error) {
		return local_msg_tab.Msg{Key: "k", Topic: "t", Content: "c"}, nil
	})
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "not_exist_db")
}
