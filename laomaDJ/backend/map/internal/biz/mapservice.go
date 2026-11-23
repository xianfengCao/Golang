// Package biz 调用高德驾车规划，把第一条路线的距离和时长交给计价服务。
// 本包不连 MySQL，也不注册到 Consul。调用方是计价服务的 gRPC 客户端。
package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"laomadj/scale"

	"github.com/go-kratos/kratos/v2/log"
)

// amapKey 是课程里的高德 Web 服务 key，只用于学习驾车距离。
// 它和前端 main.ts 里的 JS key 不是同一把。失效或配额用完时，询价会在这一层失败。
const amapKey = "4b37316ad0582dc767eaefa58cfd3f34"

// 高德按配额卖，不能按叫车次数直打。
// 相同起终点在本进程合并成一次 HTTP。连续失败 5 次后熔断 10 秒。
// 整个进程对高德每秒最多 100 次，突发 200。超过就失败，把配额留给还能完成的询价。
var (
	amapFlight  scale.Flight
	amapBreaker = scale.NewBreaker(5, 10*time.Second)
	amapLimit   = scale.NewLimiter(100, 200, 1, 1)
)

// MapServiceBiz 调用高德驾车接口。返回值单位是米和秒，都是十进制字符串，和计价服务约定一致。
type MapServiceBiz struct {
	log    *log.Helper
	client *http.Client
}

// NewMapServiceBiz 供 wire 组装。
// HTTP 客户端超时 8 秒。Transport 上的 MaxConnsPerHost 是打向高德的连接池上限，
// 空闲连接会复用，不会为每次询价新建一条 TCP。
func NewMapServiceBiz(logger log.Logger) *MapServiceBiz {
	return &MapServiceBiz{
		log: log.NewHelper(logger),
		client: &http.Client{
			Timeout: 8 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 64,
				MaxConnsPerHost:     64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// GetDrivingInfo 请求高德驾车规划，只取第一条路线。
// origin、destination 的格式是「经度,纬度」，例如 116.304388,39.984405。顺序是经度在前。
// extensions=base 只要距离和时长，不要逐步导航。响应体最多读 1MB，避免异常大包占满内存。
// status 不是 "1"、没有路线、或距离时长为空，都返回错误，调用方不会把空串当成 0 公里。
func (msbiz *MapServiceBiz) GetDrivingInfo(ctx context.Context, origin, destination string) (string, string, error) {
	if origin == "" || destination == "" {
		return "", "", fmt.Errorf("起点或终点为空")
	}
	key := origin + "|" + destination
	// 限流放在合并之后的那一次真正调用里。同一路线的并发请求只扣 1 个令牌。
	v, err := amapFlight.Do(key, func() (interface{}, error) {
		if !amapLimit.Allow(0) {
			return nil, fmt.Errorf("地图请求过多")
		}
		var pair [2]string
		err := amapBreaker.Do(func() error {
			callCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			distance, duration, err := msbiz.fetchDriving(callCtx, origin, destination)
			if err != nil {
				return err
			}
			pair = [2]string{distance, duration}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return pair, nil
	})
	if err != nil {
		return "", "", err
	}
	pair := v.([2]string)
	return pair[0], pair[1], nil
}

func (msbiz *MapServiceBiz) fetchDriving(ctx context.Context, origin, destination string) (string, string, error) {
	query := url.Values{}
	query.Set("origin", origin)
	query.Set("destination", destination)
	query.Set("extensions", "base")
	query.Set("output", "json")
	query.Set("key", amapKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://restapi.amap.com/v3/direction/driving?"+query.Encode(), nil)
	if err != nil {
		return "", "", err
	}
	resp, err := msbiz.client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("高德返回 HTTP %d", resp.StatusCode)
	}

	var parsed directionDrivingResp
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", "", err
	}
	if parsed.Status != "1" {
		info := parsed.Info
		if info == "" {
			info = "高德驾车规划失败"
		}
		return "", "", fmt.Errorf("%s", info)
	}
	if len(parsed.Route.Paths) == 0 {
		return "", "", fmt.Errorf("高德没有返回路线")
	}
	path := parsed.Route.Paths[0]
	if path.Distance == "" || path.Duration == "" {
		return "", "", fmt.Errorf("高德路线缺少距离或时长")
	}
	return path.Distance, path.Duration, nil
}

// directionDrivingResp 只保留计价需要的字段。高德还有路况、步骤，这里不解析。
// Distance 单位米，Duration 单位秒，JSON 里就是字符串。
type directionDrivingResp struct {
	Status string `json:"status"`
	Info   string `json:"info"`
	Route  struct {
		Paths []struct {
			Distance string `json:"distance"`
			Duration string `json:"duration"`
		} `json:"paths"`
	} `json:"route"`
}
