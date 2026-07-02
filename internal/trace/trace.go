// Package trace 提供全链路 trace_id 的注入与提取能力
// 供 middleware（HTTP入口）、mq（Kafka生产/消费）、service（业务层）共享使用
package trace

import "context"

// contextKey 是 context.Context 中存储 traceID 的 key（不导出，避免外部包冲突）
type contextKey struct{}

// FromContext 从 context.Context 中提取 trace_id
func FromContext(ctx context.Context) string {
	if tid, ok := ctx.Value(contextKey{}).(string); ok {
		return tid
	}
	return ""
}

// NewContext 将 trace_id 注入 context.Context
func NewContext(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, contextKey{}, traceID)
}
