package common

import (
	"fmt"
	"log"

	"git.mashibing.com/msb_47094/shopping-comm/scale"
)

// Events 是五个服务进程各自的内存队列。
// 库存变更、加购成功这类「请求已经完成、但还想通知别人」的事放这里，不占用下单协程。
// 每个进程只能看见自己发出的事件。跨服务的可靠投递要换成 Kafka，分区键仍然用这里的 Key
// （用户 ID 或 SKU ID），这样同一个键的事件顺序不变。
//
// 32 个分区、每区 1 个工人。工人只打日志。生产环境的订阅者应是异步写审计或发通知，并且要能承受重复投递。
var Events = scale.NewBus(scale.BusConfig{
	Name:       "goshop",
	Partitions: 32,
	QueueSize:  1024,
	Workers:    1,
})

// StockRing 把 SKU 映射到逻辑分片名。当前所有分片仍在同一个 MySQL 里，
// 扣库存的原子性靠 SQL 的一行更新，不靠这个环。环回答的是：如果库存表按 SKU 拆成多库，这一行该去哪一片。
var StockRing = scale.NewRing(16)

func init() {
	for i := 0; i < 64; i++ {
		StockRing.Add(fmt.Sprintf("sku-shard-%02d", i))
	}
	Events.Subscribe("stock", func(m scale.Message) {
		log.Printf("库存事件 key=%d %s", m.Key, m.Body)
	})
	Events.Subscribe("cart", func(m scale.Message) {
		log.Printf("购物车事件 key=%d %s", m.Key, m.Body)
	})
	Events.Start()
}

// Emit 把一条事件放进本进程队列。队列满时只记日志，不把失败退回给已经成功的业务。
// 调用方要把「用户已经看到成功」和「旁路通知」分开。通知丢了可以靠对账补，订单本身不能靠通知来成立。
func Emit(topic string, key int64, body string) {
	if err := Events.Publish(scale.Message{Topic: topic, Key: key, Body: []byte(body)}); err != nil {
		log.Printf("事件队列已满 topic=%s key=%d: %v", topic, key, err)
	}
}
