package scale

import (
	"hash/fnv"
	"sync"
)

// Machine 是一张允许的状态边。业务上的「待支付只能关闭或支付」都落成边，不散落在 if 里。
//
// sameOK 为 true 时，from 和 to 相同也算允许。支付回调用得上：同一条通知投递两次，
// 第二次看到状态已经是目标值，应当成功而不是报错。
// 状态用字符串。整数状态在放进来之前先格式化，避免同一张表里混用两种类型。
type Machine struct {
	sameOK bool
	edges  map[string]struct{}
}

// NewMachine 建立一张空的状态机。接下来用 Allow 加边。
func NewMachine(sameOK bool) *Machine {
	return &Machine{sameOK: sameOK, edges: make(map[string]struct{})}
}

// Allow 增加一条 from 到 to 的边，并返回自己，方便链式写完所有规则。
func (m *Machine) Allow(from, to string) *Machine {
	m.edges[from+"\x00"+to] = struct{}{}
	return m
}

// Can 判断这次变化是否被规则允许。相同状态是否允许由 sameOK 单独决定，不需要把对角线都加进边里。
func (m *Machine) Can(from, to string) bool {
	if from == to {
		return m.sameOK
	}
	_, ok := m.edges[from+"\x00"+to]
	return ok
}

// Chain 是职责链。请求依次走过每一步，某一步返回错误就停，后面的步骤不会执行。
//
// 加购前面要先限流、再看登录、再查库存。这些检查各自独立，用链把它们串起来之后，
// 新增一种检查只是多一个 Step，不用把判断嵌进原来的函数里。
type Chain struct {
	steps []Step
}

// Step 是链上的一环。Bag 用来在步骤之间传递这次请求的数据。
type Step func(bag *Bag) error

// Bag 是一次请求在链上携带的口袋。Values 给步骤存放中间结果，例如已经查到的用户。
type Bag struct {
	UserID int64
	Key    string
	Values map[string]interface{}
}

// NewChain 按给定顺序组成链。nil 步骤会被跳过。
func NewChain(steps ...Step) Chain {
	out := make([]Step, 0, len(steps))
	for _, s := range steps {
		if s != nil {
			out = append(out, s)
		}
	}
	return Chain{steps: out}
}

// Run 从头执行。bag 为 nil 时直接返回，避免步骤里对空指针解引用。
func (c Chain) Run(bag *Bag) error {
	if bag == nil {
		return errString("scale: 空的职责链上下文")
	}
	for _, s := range c.steps {
		if err := s(bag); err != nil {
			return err
		}
	}
	return nil
}

// Ring 是一致性哈希环。扩容或缩容时，只需要把落到相邻节点上的那一小段键迁走，
// 而不是把所有用户重新取模分配一遍。
//
// 聊天网关、库存分片都用它：用户 ID 或 SKU ID 先映射到环上的某个虚拟节点，
// 虚拟节点再指向真实进程或真实分表。单机运行时环上可以只有一个节点，结果永远是它自己。
type Ring struct {
	mu       sync.RWMutex
	replicas int
	nodes    []string
	hashes   []uint32
}

// NewRing 建立空环。replicas 是每个物理节点放多少个虚拟节点。
// 虚拟节点越多，键的分布越均匀。小于 1 时按 1。
func NewRing(replicas int) *Ring {
	if replicas < 1 {
		replicas = 1
	}
	return &Ring{replicas: replicas}
}

// Add 把一个物理节点挂上环。重复添加同一个名字会再挂一批虚拟节点，调用方应保证名字只加一次。
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.replicas; i++ {
		h := hash32(node + "#" + itoa(i))
		pos := 0
		for pos < len(r.hashes) && r.hashes[pos] < h {
			pos++
		}
		r.hashes = append(r.hashes, 0)
		r.nodes = append(r.nodes, "")
		copy(r.hashes[pos+1:], r.hashes[pos:])
		copy(r.nodes[pos+1:], r.nodes[pos:])
		r.hashes[pos] = h
		r.nodes[pos] = node
	}
}

// Get 返回 key 应该落到的物理节点。环是空的时候返回空字符串。
func (r *Ring) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return ""
	}
	h := hash32(key)
	pos := 0
	for pos < len(r.hashes) && r.hashes[pos] < h {
		pos++
	}
	if pos == len(r.hashes) {
		pos = 0
	}
	return r.nodes[pos]
}

// BytePool 复用短命的字节缓冲，减轻 10 亿次分配带来的 GC 压力。
//
// 只适合「取出、写完、立刻归还」的缓冲。跨协程交给别人慢慢写的字节（例如 WebSocket 帧）
// 不能放进池里：归还之后下一次 Get 会把同一块内存发给别人，造成内容串线。
type BytePool struct {
	p sync.Pool
}

// NewBytePool 建立缓冲池。每块缓冲的容量是 size，不够用的调用方应自己分配，不要把更大的块还回来。
func NewBytePool(size int) *BytePool {
	if size < 1 {
		size = 1
	}
	return &BytePool{p: sync.Pool{New: func() interface{} {
		b := make([]byte, size)
		return &b
	}}}
}

// Get 取出一块缓冲。长度等于容量，调用方按需要自行切片。
func (p *BytePool) Get() *[]byte {
	return p.p.Get().(*[]byte)
}

// Put 归还缓冲。会把长度恢复成容量，避免下一次拿到一条被截短的切片。
func (p *BytePool) Put(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	*b = (*b)[:cap(*b)]
	p.p.Put(b)
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func pow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
		if p <= 0 {
			return 1 << 30
		}
	}
	return p
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
