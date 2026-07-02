package mq

import (
	"context"
	"encoding/json"
	"sync"
)

// MockProducer 内存Mock Kafka生产者 (用于测试)
// 记录所有发送的消息, 替代真实Kafka连接
type MockProducer struct {
	mu       sync.RWMutex
	messages []MockMessage
	sendErr  error // 模拟发送失败
	closed   bool
}

// MockMessage Mock消息体
type MockMessage struct {
	Key   string
	Value []byte
	Topic string
}

// NewMockProducer 创建Mock生产者
func NewMockProducer() *MockProducer {
	return &MockProducer{
		messages: make([]MockMessage, 0),
	}
}

// Send 发送消息到内存队列
func (mp *MockProducer) Send(ctx context.Context, key string, value interface{}) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.closed {
		return nil
	}
	if mp.sendErr != nil {
		return mp.sendErr
	}

	data, _ := json.Marshal(value)
	mp.messages = append(mp.messages, MockMessage{
		Key:   key,
		Value: data,
	})
	return nil
}

// SendToDLQ 写入死信队列
func (mp *MockProducer) SendToDLQ(ctx context.Context, key string, value interface{}) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.closed {
		return nil
	}

	data, _ := json.Marshal(value)
	mp.messages = append(mp.messages, MockMessage{
		Key:   key,
		Value: data,
		Topic: "dlq",
	})
	return nil
}

// Messages 获取所有已发送的消息
func (mp *MockProducer) Messages() []MockMessage {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	cp := make([]MockMessage, len(mp.messages))
	copy(cp, mp.messages)
	return cp
}

// MessageCount 已发送消息数
func (mp *MockProducer) MessageCount() int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return len(mp.messages)
}

// SetSendError 设置模拟错误 (用于测试失败场景)
func (mp *MockProducer) SetSendError(err error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.sendErr = err
}

// Clear 清空消息记录
func (mp *MockProducer) Clear() {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.messages = mp.messages[:0]
	mp.sendErr = nil
}

// Close 关闭
func (mp *MockProducer) Close() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.closed = true
	return nil
}
