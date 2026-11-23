package scale

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// ErrPoolClosed 表示池已经关掉，不能再取连接。
var ErrPoolClosed = errors.New("scale: 连接池已关闭")

// Resource 是池里的一条连接。Alive 用来在借出前丢掉已经被对端掐掉的连接。
// database/sql 和 go-redis 自己内部已经是池，不必再套这一层。
// 这一层给的是「我们自己拨出去的连接」：下游 TCP、临时客户端，借出后必须 Put 或 Close。
type Resource interface {
	Close() error
	Alive() bool
}

// Pool 是有上限的连接池。
//
// 10 亿并发如果每人新建一条下游连接，文件描述符和下游都会先死。
// 池子把「同时打向下游的连接数」限制在 maxOpen。借不到时返回 ErrFull，由调用方拒绝请求。
// 空闲连接放在缓冲通道里，缓冲长度就是 maxIdle，多出来的归还会被关掉，避免空闲连接无限堆积。
type Pool struct {
	factory func(context.Context) (Resource, error)
	idle    chan Resource
	maxOpen int

	mu     sync.Mutex
	open   int
	closed bool
}

// NewPool 创建连接池。factory 在需要新连接时调用，它自己要设置拨号超时。
// maxOpen 小于 1 时按 1。maxIdle 大于 maxOpen 时被收成 maxOpen，因为空闲数不可能超过总连接数。
func NewPool(factory func(context.Context) (Resource, error), maxOpen, maxIdle int) *Pool {
	if maxOpen < 1 {
		maxOpen = 1
	}
	if maxIdle < 0 {
		maxIdle = 0
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	return &Pool{
		factory: factory,
		idle:    make(chan Resource, maxIdle),
		maxOpen: maxOpen,
	}
}

// Get 借一条连接。池已满时返回 ErrFull，不排队。
// 排队看起来更「努力」，但在过载时会把请求堆在内存里，延迟变成几十秒，最后全部超时。
// 立刻失败可以让客户端退避，把容量留给还能做完的请求。
func (p *Pool) Get(ctx context.Context) (Resource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	select {
	case r := <-p.idle:
		p.mu.Unlock()
		if r != nil && r.Alive() {
			return r, nil
		}
		if r != nil {
			_ = r.Close()
		}
		p.mu.Lock()
		p.open--
		p.mu.Unlock()
		return p.Get(ctx)
	default:
	}
	if p.open >= p.maxOpen {
		p.mu.Unlock()
		return nil, ErrFull
	}
	p.open++
	p.mu.Unlock()

	r, err := p.factory(ctx)
	if err != nil {
		p.mu.Lock()
		p.open--
		p.mu.Unlock()
		return nil, err
	}
	return r, nil
}

// Put 归还连接。池已关闭、连接已死、或空闲位已满时，直接关掉并释放名额。
func (p *Pool) Put(r Resource) {
	if r == nil {
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed || !r.Alive() {
		_ = r.Close()
		p.release()
		return
	}
	select {
	case p.idle <- r:
	default:
		_ = r.Close()
		p.release()
	}
}

// Close 拒绝新的借出，并关掉还躺在空闲队列里的连接。已经借出的由持有者负责 Put 或 Close。
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	for {
		select {
		case r := <-p.idle:
			if r != nil {
				_ = r.Close()
			}
			p.release()
		default:
			return
		}
	}
}

func (p *Pool) release() {
	p.mu.Lock()
	if p.open > 0 {
		p.open--
	}
	p.mu.Unlock()
}

// SQLPlan 是 database/sql 连接池的一组上限，外加「这些数字在 10 亿并发下是怎么算出来的」。
// MaxOpen 等四个字段会写进 *sql.DB。后面四个字段只用于说明，不参与拨号。
type SQLPlan struct {
	MaxOpen     int
	MaxIdle     int
	MaxLife     time.Duration
	MaxIdleTime time.Duration

	ClusterTarget int64 // 集群目标并发，这里固定按 10 亿来解释
	DBShards      int   // 设想的库分片数
	ProcsPerShard int   // 每个分片上同时跑的进程数
	ConnsPerDB    int   // 单个库实例允许的连接上限
}

// LocalSQL 给出本机学习时真正会用的池子，以及集群演算用的分母。
//
// 演算：10 亿并发不能共用一个库。按 4096 个分片拆开，每个库实例大约 2000 条连接，
// 每个分片 16 个进程，则每进程理论值约 2000/16 = 125 条。
// 本机 MySQL 的 max_connections 通常只有 151，还要同时跑聊天、商城、代驾好几个进程，
// 所以单进程实际只开 32 条。连接寿命 30 秒，是为了躲开本机 MySQL 掐掉空闲连接之后
// 继续拿着一条已经断开的连接。
func LocalSQL() SQLPlan {
	return SQLPlan{
		MaxOpen:       32,
		MaxIdle:       8,
		MaxLife:       30 * time.Second,
		MaxIdleTime:   15 * time.Second,
		ClusterTarget: 1000_000_000,
		DBShards:      4096,
		ProcsPerShard: 16,
		ConnsPerDB:    2000,
	}
}

// PerProcessConns 返回「如果真的按上面的分片部署，每个进程可以分到多少条库连接」。
// 它是演算结果，不是本机 MaxOpen。本机 MaxOpen 更小，见 LocalSQL 的注释。
func (p SQLPlan) PerProcessConns() int {
	if p.ProcsPerShard <= 0 {
		return 0
	}
	return p.ConnsPerDB / p.ProcsPerShard
}

// ApplySQL 把计划写进标准库连接池。db 为 nil 时什么都不做。
// 调用之后不要再把 MaxOpen 改成一个很大的数。池子的意义就是上限。
func ApplySQL(db *sql.DB, p SQLPlan) {
	if db == nil {
		return
	}
	if p.MaxOpen > 0 {
		db.SetMaxOpenConns(p.MaxOpen)
	}
	if p.MaxIdle > 0 {
		db.SetMaxIdleConns(p.MaxIdle)
	}
	if p.MaxLife > 0 {
		db.SetConnMaxLifetime(p.MaxLife)
	}
	if p.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(p.MaxIdleTime)
	}
}

// RedisPlan 是 Redis 客户端连接池的上限。Redis 同样不能按在线人数建连接。
// 一个进程几十到一两百条复用连接，后面靠流水线和分区扛 QPS。
type RedisPlan struct {
	PoolSize    int
	MinIdle     int
	PoolTimeout time.Duration
	IdleTimeout time.Duration
}

// LocalRedis 是本机 Redis 的池子。PoolTimeout 是「池里暂时没有空闲连接时最多等多久」。
// 等到超时就让这次命令失败，避免请求协程无限期地堵在池子门口。
func LocalRedis() RedisPlan {
	return RedisPlan{
		PoolSize:    64,
		MinIdle:     8,
		PoolTimeout: 2 * time.Second,
		IdleTimeout: 3 * time.Minute,
	}
}
