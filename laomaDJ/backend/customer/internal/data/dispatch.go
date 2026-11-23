package data

import (
	"log"
	"strconv"

	"customer/internal/biz"
	"laomadj/scale"
)

// tripBus 是本进程的派单队列。叫车写入数据库之后把行程 ID 放进来，工人只负责记下「有一单等着派」。
//
// 乘客接口不等待司机匹配。10 亿次叫车如果在 HTTP 协程里同步找司机，乘客超时和司机锁会缠在一起。
// 进程重启会丢掉还在内存里的队列。已经落库的 wait_driver 订单还在，恢复时可以按状态把它们重新塞进队列。
// 多机部署时这里应换成按城市分区的 Kafka，司机服务消费，而不是在乘客进程里打日志。
var tripBus = scale.NewBus(scale.BusConfig{
	Name:       "trip-dispatch",
	Partitions: 16,
	QueueSize:  1024,
	Workers:    1,
})

func init() {
	tripBus.Subscribe("trip", func(m scale.Message) {
		log.Printf("派单队列收到行程 %d，状态 %s", m.Key, m.Body)
	})
	tripBus.Start()
}

func enqueueTrip(row *biz.TripOrder) {
	if row == nil {
		return
	}
	err := tripBus.Publish(scale.Message{
		Topic: "trip",
		Key:   int64(row.ID),
		Body:  []byte(row.Status + " customer=" + strconv.FormatUint(uint64(row.CustomerID), 10)),
	})
	if err != nil {
		log.Printf("派单队列已满，行程 %d 已入库，需要稍后按 wait_driver 重新投递: %v", row.ID, err)
	}
}
