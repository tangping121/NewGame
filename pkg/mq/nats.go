// Package mq 封装 NATS 连接；主题名与载荷定义见 pkg/protocol/nats.go。
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"bastion/pkg/protocol"
)

// Connect 连接 NATS 服务器。
//
// 参数:
//   - url: 如 nats://127.0.0.1:4222
func Connect(url string) (*nats.Conn, error) {
	return nats.Connect(
		url,
		nats.Name("bastion"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5*time.Second),
	)
}

const eventsStream = "NEWGAME_EVENTS"
const maxDeliveries = 10

// Event is the versioned envelope used by all durable consumers.
type Event struct {
	EventID          string          `json:"event_id"`
	AggregateID      string          `json:"aggregate_id,omitempty"`
	AggregateVersion int64           `json:"aggregate_version,omitempty"`
	OccurredAt       time.Time       `json:"occurred_at"`
	Data             json.RawMessage `json:"data"`
}

func jetStream(nc *nats.Conn) (nats.JetStreamContext, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection required")
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(1024))
	if err != nil {
		return nil, err
	}
	if _, err := js.StreamInfo(eventsStream); err != nil {
		if _, addErr := js.AddStream(&nats.StreamConfig{
			Name:      eventsStream,
			Subjects:  []string{"mail.*", "rank.*", "activity.*", "economy.*", "_DLQ.>"},
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
			MaxAge:    7 * 24 * time.Hour,
			Replicas:  1,
		}); addErr != nil && addErr != nats.ErrStreamNameAlreadyInUse {
			return nil, addErr
		}
	}
	return js, nil
}

type deadLetter struct {
	Subject    string    `json:"subject"`
	Reason     string    `json:"reason"`
	Deliveries uint64    `json:"deliveries"`
	FailedAt   time.Time `json:"failed_at"`
	Data       []byte    `json:"data"`
}

func publishDeadLetter(
	js nats.JetStreamContext, msg *nats.Msg, reason string, deliveries uint64,
) error {
	body, err := json.Marshal(deadLetter{
		Subject: msg.Subject, Reason: reason, Deliveries: deliveries,
		FailedAt: time.Now().UTC(), Data: msg.Data,
	})
	if err != nil {
		return err
	}
	dlq := nats.NewMsg("_DLQ." + msg.Subject)
	dlq.Data = body
	if metadata, metaErr := msg.Metadata(); metaErr == nil {
		dlq.Header.Set(nats.MsgIdHdr,
			fmt.Sprintf("dlq:%s:%d", metadata.Stream, metadata.Sequence.Stream))
	}
	_, err = js.PublishMsg(dlq)
	return err
}

// Publish waits for JetStream persistence acknowledgement.
func Publish(ctx context.Context, nc *nats.Conn, subject string, event Event) error {
	if event.EventID == "" {
		return fmt.Errorf("event_id required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	js, err := jetStream(nc)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = body
	msg.Header.Set(nats.MsgIdHdr, event.EventID)
	_, err = js.PublishMsg(msg, nats.Context(ctx))
	return err
}

// SubscribeDurable creates a shared durable queue consumer. Messages are
// acknowledged only after handler succeeds and are retried otherwise.
func SubscribeDurable(
	nc *nats.Conn,
	subject, durable string,
	handler func(context.Context, Event) error,
) (*nats.Subscription, error) {
	js, err := jetStream(nc)
	if err != nil {
		return nil, err
	}
	return js.QueueSubscribe(subject, durable, func(msg *nats.Msg) {
		var event Event
		if err := json.Unmarshal(msg.Data, &event); err != nil || event.EventID == "" {
			if publishErr := publishDeadLetter(js, msg, "invalid event envelope", 1); publishErr == nil {
				_ = msg.Term()
			} else {
				_ = msg.NakWithDelay(2 * time.Second)
			}
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := handler(ctx, event)
		cancel()
		if err != nil {
			deliveries := uint64(1)
			if metadata, metaErr := msg.Metadata(); metaErr == nil {
				deliveries = metadata.NumDelivered
			}
			if deliveries >= maxDeliveries {
				if publishErr := publishDeadLetter(js, msg, err.Error(), deliveries); publishErr == nil {
					_ = msg.Term()
				}
				return
			}
			_ = msg.NakWithDelay(2 * time.Second)
			return
		}
		_ = msg.Ack()
	},
		nats.Durable(durable),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(maxDeliveries),
		nats.DeliverAll(),
	)
}

// NATS 主题常量（与 pkg/protocol 保持一致，便于 mq 包引用）。
const (
	SubjectMailSend    = protocol.SubjectMailSend
	SubjectRankUpdate  = protocol.SubjectRankUpdate
	SubjectActivityEvt = protocol.SubjectActivityEvent
)
