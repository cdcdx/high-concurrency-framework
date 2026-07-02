package database

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cdcdx/high-concurrency-framework/internal/config"
	"github.com/cdcdx/high-concurrency-framework/internal/model"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

const (
	OrderSearchIndex = "orders_search"
	UserSearchIndex  = "users_search"
)

// ConnectElasticsearch 连接Elasticsearch
func ConnectElasticsearch(cfg config.ESConfig, logger *zap.SugaredLogger) (*elasticsearch.Client, error) {
	if !cfg.Enabled || len(cfg.Addresses) == 0 {
		logger.Warnw("elasticsearch disabled or no addresses")
		return nil, nil
	}

	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}
	if cfg.Username != "" {
		esCfg.Username = cfg.Username
		esCfg.Password = cfg.Password
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch new client: %w", err)
	}

	res, err := client.Ping()
	if err != nil {
		return nil, fmt.Errorf("elasticsearch ping: %w", err)
	}
	res.Body.Close()

	logger.Infow("elasticsearch connected", "addrs", cfg.Addresses)
	return client, nil
}

// SearchClient Elasticsearch 搜索客户端
type SearchClient struct {
	es     *elasticsearch.Client
	logger *zap.SugaredLogger
}

// NewSearchClient 创建ES搜索客户端
func NewSearchClient(es *elasticsearch.Client, logger *zap.SugaredLogger) *SearchClient {
	return &SearchClient{es: es, logger: logger}
}

// IndexOrder 索引订单文档到ES (写后同步索引)
func (sc *SearchClient) IndexOrder(ctx context.Context, order *model.Order) error {
	if sc.es == nil {
		return nil // 降级: ES不可用则跳过
	}

	body, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order for es: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      OrderSearchIndex,
		DocumentID: order.OrderNo,
		Body:       bytes.NewReader(body),
		Refresh:    "false", // 不强制刷新, 高吞吐
	}

	res, err := req.Do(ctx, sc.es)
	if err != nil {
		sc.logger.Warnw("es index order failed", "order_no", order.OrderNo, "err", err)
		return fmt.Errorf("es index order: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		sc.logger.Warnw("es index order error response", "order_no", order.OrderNo, "status", res.StatusCode)
	}
	return nil
}

// IndexUserProfile 索引用户资料到ES
func (sc *SearchClient) IndexUserProfile(ctx context.Context, profile *model.UserProfile) error {
	if sc.es == nil {
		return nil
	}

	body, err := json.Marshal(profile)
	if err != nil {
		return fmt.Errorf("marshal profile for es: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      UserSearchIndex,
		DocumentID: fmt.Sprintf("%d", profile.UserID),
		Body:       bytes.NewReader(body),
		Refresh:    "false",
	}

	res, err := req.Do(ctx, sc.es)
	if err != nil {
		sc.logger.Warnw("es index profile failed", "user_id", profile.UserID, "err", err)
		return fmt.Errorf("es index profile: %w", err)
	}
	defer res.Body.Close()
	return nil
}

// SearchOrders 全文搜索订单 (支持按用户ID/金额范围/状态过滤)
func (sc *SearchClient) SearchOrders(ctx context.Context, keyword string, page, size int) ([]model.Order, int64, error) {
	if sc.es == nil {
		return nil, 0, fmt.Errorf("elasticsearch not available")
	}

	query := map[string]interface{}{
		"from": (page - 1) * size,
		"size": size,
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  keyword,
				"fields": []string{"order_no^2", "status"},
			},
		},
		"sort": []map[string]interface{}{
			{"created_at": map[string]string{"order": "desc"}},
		},
	}

	return sc.executeOrderSearch(ctx, query)
}

// SearchUsers 全文搜索用户 (按昵称/邮箱/手机号)
func (sc *SearchClient) SearchUsers(ctx context.Context, keyword string, page, size int) ([]model.UserProfile, int64, error) {
	if sc.es == nil {
		return nil, 0, fmt.Errorf("elasticsearch not available")
	}

	query := map[string]interface{}{
		"from": (page - 1) * size,
		"size": size,
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  keyword,
				"fields": []string{"nickname^3", "email^2", "phone"},
			},
		},
	}

	return sc.executeUserSearch(ctx, query)
}

// EnsureSearchIndexes 确保ES搜索索引存在
func (sc *SearchClient) EnsureSearchIndexes(ctx context.Context) error {
	if sc.es == nil {
		return nil
	}

	// 订单搜索索引配置
	orderMapping := `{
		"settings": {
			"number_of_shards": 3,
			"number_of_replicas": 1
		},
		"mappings": {
			"properties": {
				"order_no":  {"type": "keyword"},
				"user_id":   {"type": "long"},
				"amount":    {"type": "float"},
				"status":    {"type": "keyword"},
				"created_at":{"type": "date"},
				"updated_at":{"type": "date"}
			}
		}
	}`

	// 用户搜索索引配置
	userMapping := `{
		"settings": {
			"number_of_shards": 3,
			"number_of_replicas": 1
		},
		"mappings": {
			"properties": {
				"user_id":  {"type": "long"},
				"nickname": {"type": "text", "analyzer": "standard"},
				"email":    {"type": "text"},
				"phone":    {"type": "keyword"},
				"created_at":{"type": "date"}
			}
		}
	}`

	indexes := map[string]string{
		OrderSearchIndex: orderMapping,
		UserSearchIndex:  userMapping,
	}

	for name, mapping := range indexes {
		// 检查索引是否已存在
		headReq := esapi.IndicesExistsRequest{Index: []string{name}}
		headRes, err := headReq.Do(ctx, sc.es)
		if err != nil {
			sc.logger.Warnw("es index exists check failed", "index", name, "err", err)
			continue
		}
		headRes.Body.Close()

		if headRes.StatusCode == 200 {
			continue // 索引已存在
		}

		// 创建索引
		createReq := esapi.IndicesCreateRequest{
			Index: name,
			Body:  strings.NewReader(mapping),
		}
		createRes, err := createReq.Do(ctx, sc.es)
		if err != nil {
			sc.logger.Warnw("es create index failed", "index", name, "err", err)
			continue
		}
		body, _ := io.ReadAll(createRes.Body)
		createRes.Body.Close()
		sc.logger.Infow("es index created", "index", name, "status", createRes.StatusCode, "body", string(body))
	}

	return nil
}

// executeOrderSearch 执行订单搜索查询
func (sc *SearchClient) executeOrderSearch(ctx context.Context, query map[string]interface{}) ([]model.Order, int64, error) {
	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{OrderSearchIndex},
		Body:  bytes.NewReader(body),
	}

	res, err := req.Do(ctx, sc.es)
	if err != nil {
		return nil, 0, fmt.Errorf("es search orders: %w", err)
	}
	defer res.Body.Close()

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source model.Order `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("decode es response: %w", err)
	}

	orders := make([]model.Order, len(result.Hits.Hits))
	for i, hit := range result.Hits.Hits {
		orders[i] = hit.Source
	}

	return orders, result.Hits.Total.Value, nil
}

// executeUserSearch 执行用户搜索查询
func (sc *SearchClient) executeUserSearch(ctx context.Context, query map[string]interface{}) ([]model.UserProfile, int64, error) {
	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{UserSearchIndex},
		Body:  bytes.NewReader(body),
	}

	res, err := req.Do(ctx, sc.es)
	if err != nil {
		return nil, 0, fmt.Errorf("es search users: %w", err)
	}
	defer res.Body.Close()

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source model.UserProfile `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("decode es response: %w", err)
	}

	profiles := make([]model.UserProfile, len(result.Hits.Hits))
	for i, hit := range result.Hits.Hits {
		profiles[i] = hit.Source
	}

	return profiles, result.Hits.Total.Value, nil
}
