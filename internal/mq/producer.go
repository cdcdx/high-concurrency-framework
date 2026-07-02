package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/trace"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// EventProducer Kafka事件生产者
// 支持: 幂等发送 / 压缩 / 重试 / 失败补偿
type EventProducer struct {
	writer     *kafka.Writer
	dlqWriter  *kafka.Writer // 复用 DLQ Writer，避免每次创建
	logger     *zap.SugaredLogger
	brokers    []string
	topic      string
	dlqTopic   string
	maxRetries int
}

// NewEventProducer 创建事件生产者
// 自动检查并创建 topic (如果不存在)
func NewEventProducer(brokers []string, topic string, maxRetries int, logger *zap.SugaredLogger) *EventProducer {
	dlqTopic := topic + ".dlq"

	// 自动创建 topic
	if err := createTopicIfNotExist(brokers, topic); err != nil {
		logger.Warnw("auto-create topic failed (may need manual creation)",
			"topic", topic,
			"err", err,
		)
	} else {
		logger.Infow("topic ready", "topic", topic)
	}

	writerConfig := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},     // 负载均衡: 最少字节
			BatchSize:    16384,                    // 16KB 微批
			BatchTimeout: 5 * time.Millisecond,     // 5ms 聚合延迟
			Compression:  kafka.Lz4,                // LZ4 压缩
			RequiredAcks: kafka.RequireAll,        // acks=all
			MaxAttempts:  maxRetries,               // 重试次数
			Async:        false,                    // 同步发送确保可靠性
		}
	}

	return &EventProducer{
		writer:     writerConfig(topic),
		dlqWriter:  writerConfig(dlqTopic),
		logger:     logger,
		brokers:    brokers,
		topic:      topic,
		dlqTopic:   dlqTopic,
		maxRetries: maxRetries,
	}
}

// Send 发送事件消息
func (ep *EventProducer) Send(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// 从 context 提取 trace_id 并注入到 Kafka Headers（用于跨服务/消费者追踪）
	headers := []kafka.Header{
		{Key: "sent_at", Value: []byte(time.Now().Format(time.RFC3339))},
	}
	if traceID := trace.FromContext(ctx); traceID != "" {
		headers = append(headers, kafka.Header{Key: "trace_id", Value: []byte(traceID)})
	}

	msg := kafka.Message{
		Key:     []byte(key),
		Value:   data,
		Time:    time.Now(),
		Headers: headers,
	}

	err = ep.writer.WriteMessages(ctx, msg)
	if err != nil {
		ep.logger.Errorw("kafka send failed, writing to compensation log",
			"topic", ep.topic,
			"key", key,
			"err", err,
		)
		// 失败补偿: 记录到本地日志 (可后续由FileBeat采集重放)
		ep.compensationLog(key, data)
		return fmt.Errorf("kafka send: %w", err)
	}

	return nil
}

// SendAsync 异步发送 (不等待确认, 适合非关键事件)
// 注意：goroutine 内使用 context.Background()，因为 HTTP 请求 ctx 会在响应结束后被取消
func (ep *EventProducer) SendAsync(ctx context.Context, key string, value interface{}) {
	// 从 ctx 提取 trace_id，避免 ctx 被取消后丢失
	traceID := trace.FromContext(ctx)
	go func() {
		// 使用独立 context，不绑定 HTTP 请求生命周期，设置 5s 超时防止卡死
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if traceID != "" {
			bgCtx = trace.NewContext(bgCtx, traceID)
		}
		if err := ep.Send(bgCtx, key, value); err != nil {
			ep.logger.Errorw("async kafka send failed", "key", key, "traceId", traceID, "err", err)
		}
	}()
}

// SendToDLQ 将失败消息写入死信队列 (复用 dlqWriter)
func (ep *EventProducer) SendToDLQ(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
		Headers: []kafka.Header{
			{Key: "original_topic", Value: []byte(ep.topic)},
			{Key: "dead_letter_at", Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}
	return ep.dlqWriter.WriteMessages(ctx, msg)
}

// Ping 检查 Kafka 连通性 (用于健康检查)
func (ep *EventProducer) Ping(ctx context.Context) error {
	if len(ep.brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	// 尝试连接第一个 broker
	conn, err := kafka.DialContext(ctx, "tcp", ep.brokers[0])
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// Close 关闭生产者
func (ep *EventProducer) Close() error {
	_ = ep.dlqWriter.Close()
	return ep.writer.Close()
}

// compensationLog 本地失败补偿日志
func (ep *EventProducer) compensationLog(key string, data []byte) {
	ep.logger.Warnw("COMPENSATION",
		"topic", ep.topic,
		"key", key,
		"payload", string(data),
		"timestamp", time.Now().Unix(),
	)
}
