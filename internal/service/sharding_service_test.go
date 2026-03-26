package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"local_msg_tab/internal/dao"
	"local_msg_tab/internal/domain"

	"github.com/IBM/sarama"
	"github.com/rermrf/emo/logger"
	"gorm.io/gorm"
	gormtests "gorm.io/gorm/utils/tests"
)

type recordingProducer struct {
	sentMsg   *sarama.ProducerMessage
	sendErr   error
	batchMsgs []*sarama.ProducerMessage
	batchErr  error
}

func (r *recordingProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	r.sentMsg = msg
	return 0, 0, r.sendErr
}

func (r *recordingProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	r.batchMsgs = msgs
	return r.batchErr
}

func (r *recordingProducer) Close() error { return nil }

func (r *recordingProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }

func (r *recordingProducer) IsTransactional() bool { return false }

func (r *recordingProducer) BeginTxn() error { return nil }

func (r *recordingProducer) CommitTxn() error { return nil }

func (r *recordingProducer) AbortTxn() error { return nil }

func (r *recordingProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (r *recordingProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

type recordingConnPool struct {
	query string
	args  []interface{}
}

func (r *recordingConnPool) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, nil
}

func (r *recordingConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	r.query = query
	r.args = append([]interface{}(nil), args...)
	return recordingResult(1), nil
}

func (r *recordingConnPool) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (r *recordingConnPool) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

type recordingResult int64

func (r recordingResult) LastInsertId() (int64, error) { return 0, nil }

func (r recordingResult) RowsAffected() (int64, error) { return int64(r), nil }

func newTestDB(t *testing.T, connPool gorm.ConnPool) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(gormtests.DummyDialector{}, &gorm.Config{
		ConnPool:             connPool,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open gorm db: %v", err)
	}
	return db
}

func TestGetPartitionMsgsMatchesErrorsByMessageID(t *testing.T) {
	svc := &ShardingService{MaxTimes: 3}
	dmsgs := []dao.LocalMsg{
		{ID: 1, Key: "dup-key", SendTimes: 0},
		{ID: 2, Key: "dup-key", SendTimes: 0},
	}
	errs := sarama.ProducerErrors{
		&sarama.ProducerError{
			Msg: &sarama.ProducerMessage{
				Key:      sarama.StringEncoder("dup-key"),
				Metadata: int64(1),
			},
			Err: errors.New("send failed"),
		},
	}

	failMsgs, initMsgs, successMsgs, err := svc.getPartitionMsgs(dmsgs, errs)
	if err != nil {
		t.Fatalf("getPartitionMsgs returned error: %v", err)
	}
	if len(failMsgs) != 0 {
		t.Fatalf("expected no failed msgs before reaching max retries, got %d", len(failMsgs))
	}
	if len(initMsgs) != 1 || initMsgs[0].ID != 1 {
		t.Fatalf("expected only ID 1 to stay init, got %+v", initMsgs)
	}
	if len(successMsgs) != 1 || successMsgs[0].ID != 2 {
		t.Fatalf("expected only ID 2 to stay success, got %+v", successMsgs)
	}
}

func TestUpdateMsgsUsesIDsInWhereClause(t *testing.T) {
	connPool := &recordingConnPool{}
	db := newTestDB(t, connPool)
	svc := &ShardingService{}

	err := svc.updateMsgs(context.Background(), db, "local_msgs", []dao.LocalMsg{
		{ID: 1, Key: "dup-key"},
		{ID: 2, Key: "dup-key"},
	}, map[string]interface{}{
		"status": dao.MsgStatusSuccess,
	}, "topic-a")
	if err != nil {
		t.Fatalf("updateMsgs returned error: %v", err)
	}

	if !strings.Contains(connPool.query, "WHERE id IN (?,?)") && !strings.Contains(connPool.query, "WHERE `id` IN (?,?)") {
		t.Fatalf("expected update to target ids, query=%q", connPool.query)
	}
	if strings.Contains(connPool.query, "`key`") {
		t.Fatalf("expected update query to avoid key filtering, query=%q", connPool.query)
	}
}

func TestSendMsgPreservesKafkaRoutingFields(t *testing.T) {
	connPool := &recordingConnPool{}
	db := newTestDB(t, connPool)
	producer := &recordingProducer{}
	svc := &ShardingService{
		producer: producer,
		MaxTimes: 3,
		logger:   logger.NewNopLogger(),
	}

	data, err := json.Marshal(domain.Msg{
		Topic:     "topic-a",
		Key:       "order-1",
		Partition: 7,
		Content:   "payload",
	})
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}

	err = svc.sendMsg(context.Background(), db, "local_msgs", dao.LocalMsg{
		ID:   1,
		Key:  "order-1",
		Data: data,
	})
	if err != nil {
		t.Fatalf("sendMsg returned error: %v", err)
	}
	if producer.sentMsg == nil {
		t.Fatal("expected producer to receive a message")
	}
	if producer.sentMsg.Topic != "topic-a" {
		t.Fatalf("unexpected topic %q", producer.sentMsg.Topic)
	}
	if producer.sentMsg.Partition != 7 {
		t.Fatalf("expected partition 7, got %d", producer.sentMsg.Partition)
	}
	keyBytes, err := producer.sentMsg.Key.Encode()
	if err != nil {
		t.Fatalf("encode key: %v", err)
	}
	if string(keyBytes) != "order-1" {
		t.Fatalf("expected key order-1, got %q", string(keyBytes))
	}
	valueBytes, err := producer.sentMsg.Value.Encode()
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	if string(valueBytes) != "payload" {
		t.Fatalf("expected payload, got %q", string(valueBytes))
	}
}
