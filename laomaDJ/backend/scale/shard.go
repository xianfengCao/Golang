package scale

import "sync"

// Int64Map 是按 int64 键分片的并发映射。
//
// 一把全局读写锁在 10 亿连接的登记/心跳里会变成所有请求排队的地方。
// 这里把键散列到固定数量的小锁上：不同用户的上下线几乎不互相等待，
// 同一个用户的替换和删除仍然落在同一把锁里，所以「后连上的顶掉先连上的」仍然是原子的。
//
// 分片数必须是 2 的幂。用与运算取分片，比取模少一次除法，在热路径上值得这样做。
// 分片不是越多越好。每个分片都有自己的 map 和锁，过多会增加内存，也让一次全量扫描要拿更多次锁。
// 256 到 1024 对单进程的在线表通常够用。
type Int64Map struct {
	shards []int64Shard
	mask   uint64
}

type int64Shard struct {
	mu sync.RWMutex
	m  map[int64]interface{}
}

// NewInt64Map 建立分片表。shards 小于 1 时按 1 处理，不是 2 的幂时向上取整。
func NewInt64Map(shards int) *Int64Map {
	n := pow2(shards)
	out := &Int64Map{
		shards: make([]int64Shard, n),
		mask:   uint64(n - 1),
	}
	for i := range out.shards {
		out.shards[i].m = make(map[int64]interface{})
	}
	return out
}

func (m *Int64Map) shard(key int64) *int64Shard {
	return &m.shards[mix64(uint64(key))&m.mask]
}

// Get 读取键。没登记过时 ok 为 false。
// 读锁只挡住同一分片上的写，其它分片的上下线照常进行。
func (m *Int64Map) Get(key int64) (interface{}, bool) {
	s := m.shard(key)
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

// Swap 写入新值并返回旧值。had 表示这个键原来有没有值。
// 调用方可以据此把被顶替的旧连接异步关掉，而不必先删再插，避免中间出现「这个用户不在线」的空窗。
func (m *Int64Map) Swap(key int64, val interface{}) (old interface{}, had bool) {
	s := m.shard(key)
	s.mu.Lock()
	old, had = s.m[key]
	s.m[key] = val
	s.mu.Unlock()
	return old, had
}

// DeleteIf 只有在键还在、并且 match 返回 true 时才删除。
// 连接关闭和「新连接已经顶上来」会同时发生。旧连接只能删掉自己，不能把新连接一起删掉。
// match 在持有该分片写锁时执行，里面不要再去拿别的锁，也不要做网络 IO。
func (m *Int64Map) DeleteIf(key int64, match func(interface{}) bool) bool {
	s := m.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[key]
	if !ok || match == nil || !match(cur) {
		return false
	}
	delete(s.m, key)
	return true
}

// Range 逐个分片扫描。回调期间拿着该分片的读锁，所以回调里不要写这张表，也不要阻塞。
// 需要关闭连接时，先把对象收进切片，释放锁之后再关。
func (m *Int64Map) Range(fn func(int64, interface{})) {
	if fn == nil {
		return
	}
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.RLock()
		for k, v := range s.m {
			fn(k, v)
		}
		s.mu.RUnlock()
	}
}

// Len 返回当前元素个数。要依次读每片的锁，只适合监控，不适合放在每条消息的路径上。
func (m *Int64Map) Len() int {
	n := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}
