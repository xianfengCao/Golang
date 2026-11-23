package scale

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
)

// 队列相关的错误。调用方看到 ErrFull 应该拒绝或降级，不要改成阻塞等待。
// 阻塞等待会把下游的慢反压回所有在线连接，10 亿规模下表现为整站卡死。
var (
	ErrFull   = errors.New("scale: 队列已满")
	ErrClosed = errors.New("scale: 队列已关闭")
)

// Message 是队列里的一条消息。
// Topic 决定交给哪一组订阅者。Key 决定进哪个分区：相同 Key 永远进同一分区。
// Body 的所有权在 Publish 成功返回后交给队列，调用方不能再改这块内存。
type Message struct {
	Topic string
	Key   int64
	Body  []byte
}

// Handler 是一个订阅者。同一个分区里的回调按到达顺序执行。
// 回调要快。里面如果再去做外部 RPC，等于把工人数量变成了下游的并发上限。
type Handler func(Message)

// BusConfig 决定这条队列能吞多少、用多少工人。
//
// Partitions：分区数，会向上取成 2 的幂。同一 Key 的消息只会出现在一个分区，因此分区内有序。
// QueueSize：每个分区的缓冲条数。总容量 = Partitions * QueueSize。这是内存上限，不是建议值。
// Workers：每个分区的消费协程数。要保证同 Key 有序时必须是 1。大于 1 时同一分区会并行，顺序不再成立。
type BusConfig struct {
	Name       string
	Partitions int
	QueueSize  int
	Workers    int
}

// Bus 是进程内的分区消息队列，语义对齐「按 Key 分区的 Kafka」。
//
// 它解决的问题：读连接、扣库存、叫车这些请求协程不能停下来等历史记录、通知、派单。
// 那些事放到队列里，由固定数量的工人做。工人数量按 CPU 和下游连接池来定，不按在线人数来定。
//
// 单机内存队列在进程崩溃时会丢消息。需要至少一次送达时，生产环境应换成带副本的日志（Kafka、Pulsar），
// 或者先把事件写进本库的 outbox 表，再由工人投递。分区、有界、同 Key 有序这三条约定不变。
type Bus struct {
	name    string
	parts   []chan Message
	mask    uint64
	workers int

	mu   sync.RWMutex
	subs map[string][]Handler

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	wg     sync.WaitGroup

	dropped uint64
}

// NewBus 按配置建好分区缓冲。此时还没有消费协程，Publish 会先堆在缓冲里，Start 之后才开始消费。
func NewBus(c BusConfig) *Bus {
	n := pow2(c.Partitions)
	if c.QueueSize < 1 {
		c.QueueSize = 1
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bus{
		name:    c.Name,
		parts:   make([]chan Message, n),
		mask:    uint64(n - 1),
		workers: c.Workers,
		subs:    make(map[string][]Handler),
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := range b.parts {
		b.parts[i] = make(chan Message, c.QueueSize)
	}
	return b
}

// Subscribe 注册某个 Topic 的处理函数。可以多次调用，每条消息会依次交给全部订阅者。
// 这是观察者模式：生产者只知道 Topic，不知道后面有日志、落库还是推送。
func (b *Bus) Subscribe(topic string, h Handler) {
	if h == nil {
		return
	}
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], h)
	b.mu.Unlock()
}

// Start 拉起消费协程，重复调用不会多拉起一份。
func (b *Bus) Start() {
	b.once.Do(func() {
		for _, ch := range b.parts {
			for w := 0; w < b.workers; w++ {
				b.wg.Add(1)
				go b.loop(ch)
			}
		}
	})
}

// Publish 尝试把消息放进它所属的分区。缓冲满或队列已关闭时立刻返回错误，不会堵住调用方。
// 这是热路径：聊天读协程、下单协程都在这里返回。
func (b *Bus) Publish(m Message) error {
	select {
	case <-b.ctx.Done():
		return ErrClosed
	default:
	}
	ch := b.parts[mix64(uint64(m.Key))&b.mask]
	select {
	case <-b.ctx.Done():
		return ErrClosed
	case ch <- m:
		return nil
	default:
		atomic.AddUint64(&b.dropped, 1)
		return ErrFull
	}
}

// Dropped 返回因为队列满而被拒绝的条数，给监控用。持续增长说明工人或下游已经跟不上。
func (b *Bus) Dropped() uint64 {
	return atomic.LoadUint64(&b.dropped)
}

// Close 通知工人退出并等待它们结束。缓冲里还没取走的消息会被丢掉。
func (b *Bus) Close() {
	b.cancel()
	b.wg.Wait()
}

func (b *Bus) loop(ch <-chan Message) {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case m := <-ch:
			b.safeCall(m)
		}
	}
}

func (b *Bus) safeCall(m Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scale bus %s panic: %v", b.name, r)
		}
	}()
	b.mu.RLock()
	hs := append([]Handler(nil), b.subs[m.Topic]...)
	b.mu.RUnlock()
	for _, h := range hs {
		h(m)
	}
}
