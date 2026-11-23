package scale

import (
	"sync"
	"time"
)

// Limiter 是按键分片的令牌桶。
//
// 每个键（用户、手机号、接口）有自己的桶：每秒补充 perSecond 个令牌，桶里最多 burst 个。
// 没有令牌就拒绝。这是为了让少数狂刷的键不影响其它键，而不是整个进程共用一个计数器。
//
// 键的数量不能随用户数无限增长，否则 10 亿个键会把内存吃光。
// 每个分片最多保留 maxKeysPerShard 个桶。超过时丢掉该分片里任意一个旧桶，给新键腾位置。
// 被丢掉的热键下一拍会重建，只是突发额度重新算过。这是用一点限流精度换内存上限。
type Limiter struct {
	shards  []limShard
	mask    uint64
	rate    float64
	burst   float64
	maxKeys int
}

type limShard struct {
	mu   sync.Mutex
	keys map[int64]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewLimiter 建立限流器。
// perSecond 是每秒补充的令牌数，burst 是桶容量。shards、maxKeysPerShard 小于 1 时按 1。
func NewLimiter(perSecond float64, burst int, shards int, maxKeysPerShard int) *Limiter {
	if perSecond <= 0 {
		perSecond = 1
	}
	if burst < 1 {
		burst = 1
	}
	if maxKeysPerShard < 1 {
		maxKeysPerShard = 1
	}
	n := pow2(shards)
	l := &Limiter{
		shards:  make([]limShard, n),
		mask:    uint64(n - 1),
		rate:    perSecond,
		burst:   float64(burst),
		maxKeys: maxKeysPerShard,
	}
	for i := range l.shards {
		l.shards[i].keys = make(map[int64]*bucket)
	}
	return l
}

// Allow 在当前时刻为 key 取走 1 个令牌。取到返回 true。
func (l *Limiter) Allow(key int64) bool {
	return l.allow(time.Now(), key)
}

func (l *Limiter) allow(now time.Time, key int64) bool {
	s := &l.shards[mix64(uint64(key))&l.mask]
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.keys[key]
	if b == nil {
		if len(s.keys) >= l.maxKeys {
			for k := range s.keys {
				delete(s.keys, k)
				break
			}
		}
		b = &bucket{tokens: l.burst, last: now}
		s.keys[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Breaker 是熔断器。连续失败达到阈值后进入打开状态，冷却时间内直接拒绝，不再访问下游。
//
// 下游已经超时的时候，继续把 10 亿请求打过去只会让它更慢，并且占满本进程的工人和连接池。
// 打开期间快速失败，冷却结束后放行下一次试探。试探成功就合上，试探失败就重新打开。
type Breaker struct {
	mu        sync.Mutex
	failures  int
	threshold int
	cooldown  time.Duration
	openUntil time.Time
}

// ErrBreakerOpen 表示熔断器处于打开状态，这次没有调用下游。
var ErrBreakerOpen = errString("scale: 熔断器打开")

type errString string

func (e errString) Error() string { return string(e) }

// NewBreaker 建立熔断器。threshold 是连续失败多少次后打开。cooldown 是打开后多久再试一次。
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}
	return &Breaker{threshold: threshold, cooldown: cooldown}
}

// Do 在熔断器允许时执行 fn。fn 返回错误算一次失败，返回 nil 会把失败计数清零。
func (b *Breaker) Do(fn func() error) error {
	b.mu.Lock()
	if !b.openUntil.IsZero() && time.Now().Before(b.openUntil) {
		b.mu.Unlock()
		return ErrBreakerOpen
	}
	b.mu.Unlock()

	err := fn()

	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.failures = 0
		b.openUntil = time.Time{}
		return nil
	}
	b.failures++
	if b.failures >= b.threshold {
		b.openUntil = time.Now().Add(b.cooldown)
		b.failures = 0
	}
	return err
}

// Flight 把同一个键上同时发生的调用合并成一次。
//
// 典型场景：同一条路线在同一秒被大量询价。如果每个请求都去打地图，下游会被相同的工作压垮。
// 第一个请求真正执行，后来的请求等到它结束，直接拿走同一个结果。键不同的请求互不影响。
// 这只合并「这一台进程里」的并发。多台机器各自还会打一次，要再往前就得加缓存。
type Flight struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Do 执行 fn，或等待同一个 key 上已经在执行的那次。
// fn 只会在这个键上被调用一次，即使有很多 goroutine 同时进来。
func (f *Flight) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	f.mu.Lock()
	if f.m == nil {
		f.m = make(map[string]*flightCall)
	}
	if c, ok := f.m[key]; ok {
		f.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	f.m[key] = c
	f.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	f.mu.Lock()
	delete(f.m, key)
	f.mu.Unlock()
	return c.val, c.err
}

// Dedup 在一段时间内记住见过的键。用来丢掉 UDP 回环、以及成功之后的重复提交。
//
// 不用「每个键一个 time.AfterFunc」。10 亿条消息会造出 10 亿个定时器。
// 这里按秒分槽，一整秒过期时丢掉那一槽。时间精度是 1 秒，ttl 不足 1 秒时也按 1 秒槽处理。
// 槽的数量等于 ttl 的秒数，所以内存和「见过多少键」有关，但定时器数量是零。
type Dedup struct {
	mu      sync.Mutex
	slots   []sync.Map
	slotSec []int64
	n       int
}

// NewDedup 建立一个会遗忘旧键的表。ttl 决定一个键大约保留多久。
func NewDedup(ttl time.Duration) *Dedup {
	n := int(ttl / time.Second)
	if n < 1 {
		n = 1
	}
	return &Dedup{
		slots:   make([]sync.Map, n),
		slotSec: make([]int64, n),
		n:       n,
	}
}

// Seen 只查询窗口内是否见过 key，不写入。
// 幂等要分成两步：请求开始时用 Seen 拒绝已经成功过的重复提交；业务成功之后再用 First 记住。
// 如果一开始就 First，失败的请求也会占住这个键，调用方无法重试。
func (d *Dedup) Seen(key interface{}) bool {
	now := time.Now().Unix()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen(now, key)
}

// First 在 ttl 窗口内第一次见到 key 时返回 true，之后返回 false。
// key 必须能作为 map 的键，例如 string、整数、数组，不能是切片。
func (d *Dedup) First(key interface{}) bool {
	now := time.Now().Unix()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen(now, key) {
		return false
	}
	idx := int(now % int64(d.n))
	if d.slotSec[idx] != now {
		d.slots[idx] = sync.Map{}
		d.slotSec[idx] = now
	}
	d.slots[idx].Store(key, struct{}{})
	return true
}

func (d *Dedup) seen(now int64, key interface{}) bool {
	windowStart := now - int64(d.n) + 1
	for i := range d.slots {
		if d.slotSec[i] < windowStart {
			continue
		}
		if _, ok := d.slots[i].Load(key); ok {
			return true
		}
	}
	return false
}
