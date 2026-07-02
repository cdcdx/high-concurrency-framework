package mq

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/cdcdx/high-concurrency-framework/internal/trace"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, key string, value []byte) error

// EventConsumer Kafka事件消费者
// 支持: 手动提交offset / 限速消费 / 重试3次 / 死信队列
type EventConsumer struct {
	reader      *kafka.Reader
	logger      *zap.SugaredLogger
	handler     MessageHandler
	producer    *EventProducer // 用于写入DLQ
	maxRetries  int
	rateLimiter *rate.Limiter

	concurrency int           // 记录 worker 数量（用于优雅停止）
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// NewEventConsumer 创建事件消费者
func NewEventConsumer(
	brokers []string,
	topic, groupID string,
	concurrency int,
	rateLimit int,
	maxRetries int,
	handler MessageHandler,
	dlqProducer *EventProducer,
	logger *zap.SugaredLogger,
) *EventConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1024,             // 最低1KB拉取
		MaxBytes:       10 * 1024 * 1024, // 最大10MB
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0, // 手动提交
		StartOffset:    kafka.LastOffset,
		MaxAttempts:    1, // 自行控制重试
	})

	ec := &EventConsumer{
		reader:      reader,
		logger:      logger,
		handler:     handler,
		producer:    dlqProducer,
		maxRetries:  maxRetries,
		rateLimiter: rate.NewLimiter(rate.Limit(rateLimit), rateLimit),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}, concurrency), // 缓冲大小 = worker 数量
	}
	return ec
}

// createTopicIfNotExist 检查并自动创建 topic
func createTopicIfNotExist(brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	// 检查 topic 是否已存在
	partitions, err := controllerConn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		return nil // 已存在
	}

	// 创建 topic
	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}

// Start 启动消费者 (启动 concurrency 个协程并行消费)
func (ec *EventConsumer) Start(ctx context.Context, concurrency int) {
	// 自动创建 topic
	brokers := ec.reader.Config().Brokers
	topic := ec.reader.Config().Topic
	if err := createTopicIfNotExist(brokers, topic); err != nil {
		ec.logger.Warnw("auto-create topic failed (may need manual creation)",
			"topic", topic,
			"err", err,
		)
	} else {
		ec.logger.Infow("topic ready", "topic", topic)
	}

	for i := 0; i < concurrency; i++ {
		go ec.consumeLoop(ctx, i)
	}
	ec.logger.Infow("consumer started",
		"topic", topic,
		"concurrency", concurrency,
	)
}

// consumeLoop 单协程消费循环
func (ec *EventConsumer) consumeLoop(ctx context.Context, workerID int) {
	defer func() {
		ec.doneCh <- struct{}{}
	}()

	// 将 ctx 和 stopCh 组合成一个可取消的 context，确保 stopCh 关闭时能立即停止 Wait
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-ec.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 速率限制（使用组合 context，stopCh 关闭后 Wait 立即返回）
		if err := ec.rateLimiter.Wait(ctx); err != nil {
			if !ec.isStopped() {
				ec.logger.Warnw("rate limiter wait error", "worker", workerID, "err", err)
			}
			return
		}

		msg, err := ec.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		key := string(msg.Key)
		value := msg.Value

		// 从 Kafka Headers 提取 trace_id，注入到 context 中（贯穿消费者全链路）
		traceID := extractTraceID(msg.Headers)
		msgCtx := ctx
		if traceID != "" {
			msgCtx = trace.NewContext(ctx, traceID)
		}

		// 处理消息 (带重试)
		err = ec.processWithRetry(msgCtx, key, value)

		if err != nil {
			ec.logger.Errorw("message processing exhausted retries",
				"key", key,
				"traceId", traceID,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"err", err,
			)
			// 写入死信队列
			if ec.producer != nil {
				if dlqErr := ec.producer.SendToDLQ(msgCtx, key, value); dlqErr != nil {
					ec.logger.Errorw("failed to write DLQ", "key", key, "traceId", traceID, "err", dlqErr)
				}
			}
		}

		// 无论成功失败都提交offset (避免阻塞)
		// DLQ保证了失败消息不丢失
		if commitErr := ec.reader.CommitMessages(msgCtx, msg); commitErr != nil {
			ec.logger.Errorw("commit offset failed", "worker", workerID, "traceId", traceID, "err", commitErr)
		}
	}
}

// processWithRetry 处理消息, 失败重试 (指数退避)
func (ec *EventConsumer) processWithRetry(ctx context.Context, key string, value []byte) error {
	var lastErr error
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= ec.maxRetries; attempt++ {
		if attempt > 0 {
			ec.logger.Infow("retrying message",
				"key", key,
				"attempt", attempt,
				"maxRetries", ec.maxRetries,
			)
			// 指数退避
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		err := ec.handler(ctx, key, value)
		if err == nil {
			if attempt > 0 {
				ec.logger.Infow("message retry succeeded", "key", key, "attempt", attempt)
			}
			return nil
		}
		lastErr = err
		ec.logger.Warnw("message processing failed",
			"key", key,
			"attempt", attempt,
			"err", err,
		)
	}

	return lastErr
}

// Stop 优雅停止消费者
func (ec *EventConsumer) Stop() {
	close(ec.stopCh)

	// 等待所有 worker 完成，最多 30 秒
	timeout := time.After(30 * time.Second)
	workersDone := 0
	for workersDone < ec.concurrency {
		select {
		case <-ec.doneCh:
			workersDone++
		case <-timeout:
			ec.logger.Warnw("consumer stop timeout, force closing",
				"topic", ec.reader.Config().Topic,
				"workers_done", workersDone,
				"total_workers", ec.concurrency,
			)
			goto stopReader
		}
	}
stopReader:
	ec.reader.Close()
	ec.logger.Infow("consumer stopped",
		"topic", ec.reader.Config().Topic,
		"workers_done", workersDone,
	)
}

// extractTraceID 从 Kafka Headers 中提取 trace_id
func extractTraceID(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "trace_id" {
			return string(h.Value)
		}
	}
	return ""
}

// isStopped 检查消费者是否已停止（非阻塞读取 stopCh）
func (ec *EventConsumer) isStopped() bool {
	select {
	case <-ec.stopCh:
		return true
	default:
		return false
	}
}
