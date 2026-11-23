package scale

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInt64MapSwapAndDeleteIf(t *testing.T) {
	m := NewInt64Map(8)
	m.Swap(7, "a")
	old, had := m.Swap(7, "b")
	if !had || old.(string) != "a" {
		t.Fatalf("old = %v had = %v", old, had)
	}
	if !m.DeleteIf(7, func(v interface{}) bool { return v.(string) == "b" }) {
		t.Fatal("应能删掉当前值")
	}
	if m.DeleteIf(7, func(v interface{}) bool { return true }) {
		t.Fatal("已经删掉的键不能再删")
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.Swap(int64(id), id)
		}(i)
	}
	wg.Wait()
	if m.Len() != 64 {
		t.Fatalf("len = %d", m.Len())
	}
}

func TestBusKeepsOrderForSameKey(t *testing.T) {
	b := NewBus(BusConfig{Name: "t", Partitions: 4, QueueSize: 64, Workers: 1})
	var mu sync.Mutex
	var got []int
	b.Subscribe("n", func(m Message) {
		mu.Lock()
		got = append(got, int(m.Body[0]))
		mu.Unlock()
	})
	b.Start()
	for i := 0; i < 20; i++ {
		if err := b.Publish(Message{Topic: "n", Key: 3, Body: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 20 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 20 {
		t.Fatalf("got %d", len(got))
	}
	for i := 0; i < 20; i++ {
		if got[i] != i {
			t.Fatalf("顺序被打乱: %v", got)
		}
	}
}

func TestBusRejectsWhenFull(t *testing.T) {
	b := NewBus(BusConfig{Name: "full", Partitions: 1, QueueSize: 1, Workers: 1})
	if err := b.Publish(Message{Topic: "x", Key: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(Message{Topic: "x", Key: 1}); err != ErrFull {
		t.Fatalf("err = %v", err)
	}
	if b.Dropped() != 1 {
		t.Fatalf("dropped = %d", b.Dropped())
	}
}

type fakeRes struct{ alive bool }

func (f *fakeRes) Close() error { f.alive = false; return nil }
func (f *fakeRes) Alive() bool  { return f.alive }

func TestPoolBackpressure(t *testing.T) {
	var n int32
	p := NewPool(func(ctx context.Context) (Resource, error) {
		atomic.AddInt32(&n, 1)
		return &fakeRes{alive: true}, nil
	}, 1, 1)
	r1, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Get(context.Background()); err != ErrFull {
		t.Fatalf("池满时应拒绝, err=%v", err)
	}
	p.Put(r1)
	r2, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Put(r2)
	if atomic.LoadInt32(&n) != 1 {
		t.Fatalf("应复用连接, created=%d", n)
	}
	p.Close()
	if _, err = p.Get(context.Background()); err != ErrPoolClosed {
		t.Fatalf("关闭后 err=%v", err)
	}
}

func TestLimiterBurst(t *testing.T) {
	l := NewLimiter(1, 2, 4, 8)
	if !l.Allow(9) || !l.Allow(9) {
		t.Fatal("桶容量内应放行")
	}
	if l.Allow(9) {
		t.Fatal("令牌用完应拒绝")
	}
	if !l.Allow(10) {
		t.Fatal("另一个键不应被这次拒绝连累")
	}
}

func TestBreakerOpens(t *testing.T) {
	b := NewBreaker(2, time.Hour)
	calls := 0
	fn := func() error {
		calls++
		return errString("down")
	}
	_ = b.Do(fn)
	_ = b.Do(fn)
	err := b.Do(fn)
	if err != ErrBreakerOpen {
		t.Fatalf("err=%v", err)
	}
	if calls != 2 {
		t.Fatalf("打开后不应再调用下游, calls=%d", calls)
	}
}

func TestFlightCollapses(t *testing.T) {
	var f Flight
	var calls int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := f.Do("route", func() (interface{}, error) {
				atomic.AddInt32(&calls, 1)
				time.Sleep(20 * time.Millisecond)
				return "ok", nil
			})
			if err != nil || v.(string) != "ok" {
				t.Errorf("v=%v err=%v", v, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("相同键应只执行一次, calls=%d", calls)
	}
}

func TestMachine(t *testing.T) {
	m := NewMachine(true).Allow("a", "b")
	if !m.Can("a", "b") || !m.Can("b", "b") || m.Can("b", "a") {
		t.Fatal("状态边不符合预期")
	}
}

func TestDedupWindow(t *testing.T) {
	d := NewDedup(time.Second)
	if !d.First("k") || !d.Seen("k") || d.First("k") {
		t.Fatal("窗口内第二次应被挡住")
	}
	time.Sleep(1100 * time.Millisecond)
	if !d.First("k") {
		t.Fatal("过期后应重新算第一次")
	}
}

func TestRingStable(t *testing.T) {
	r := NewRing(8)
	r.Add("a")
	r.Add("b")
	first := r.Get("user-7")
	if first != r.Get("user-7") {
		t.Fatal("同一个键应稳定落到同一个节点")
	}
	if first != "a" && first != "b" {
		t.Fatalf("node=%s", first)
	}
}

func TestLocalPlanMath(t *testing.T) {
	p := LocalSQL()
	if p.ClusterTarget != 1000_000_000 {
		t.Fatal(p.ClusterTarget)
	}
	if p.PerProcessConns() != 125 {
		t.Fatalf("每进程演算连接数 = %d", p.PerProcessConns())
	}
	if p.MaxOpen >= p.PerProcessConns() {
		t.Fatal("本机池子应小于集群演算值，避免打满学习机的 MySQL")
	}
}

func TestChainStops(t *testing.T) {
	called := false
	c := NewChain(
		func(b *Bag) error { return errString("stop") },
		func(b *Bag) error { called = true; return nil },
	)
	if err := c.Run(&Bag{}); err == nil || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

func TestBytePoolReuses(t *testing.T) {
	p := NewBytePool(8)
	b := p.Get()
	if cap(*b) != 8 {
		t.Fatal(cap(*b))
	}
	(*b)[0] = 1
	p.Put(b)
	c := p.Get()
	if cap(*c) != 8 {
		t.Fatal("归还后应还能取出缓冲")
	}
	p.Put(c)
}
