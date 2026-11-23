package models

import (
	"context"
	"encoding/json"
	"ginchat/scale"
	"ginchat/utils"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// 在线表按用户 ID 分 256 片。同一用户的顶替和删除落在同一把锁上，不同用户互不等待。
// 10 亿在线用户不会放在这一张表里：这张表只保存「连到本进程」的那一部分，用户由 gateways 哈希到某台机器。
var clients = scale.NewInt64Map(256)

// recent 挡住 UDP 广播绕回本机的第二份拷贝。按秒分槽，不给每条消息造一个定时器。
var recent = scale.NewDedup(time.Minute)

// sendLimit 按发送者限流。每个用户每秒大约 100 条，突发 200。
// 分片上的桶有上限，避免有人用海量用户 ID 把限流表撑满。
var sendLimit = scale.NewLimiter(100, 200, 256, 4096)

// chatBus 把「写 Redis」从读连接的协程上拿开。
// 32 个分区、每区缓冲 1024、每区 1 个工人：同一个用户的私聊记录在本进程内保持顺序。
// 队列满时 publishPrivate 会退回当前协程去写，避免学习环境下把消息直接丢掉。
// 生产上退回同步写会再次堵住读协程，那时应直接拒绝并让客户端重试，同时把队列换成 Kafka。
var chatBus = scale.NewBus(scale.BusConfig{
	Name:       "ginchat",
	Partitions: 32,
	QueueSize:  1024,
	Workers:    1,
})

// gateways 是聊天接入机的一致性哈希环。现在只有本进程一个节点。
// 以后每加一台机器就 Add 一次，同一个用户会稳定落在同一台上，在线表才不用全网广播。
var gateways = scale.NewRing(32)

// deliverByType 是消息类型的策略表。dispatch 按 Type 取一种投递方式。
// 私聊、群聊各自实现，互不嵌在一个 switch 里。心跳在读协程里就返回了，不会进这张表。
var deliverByType = map[int]func(Message, []byte){}

func init() {
	gateways.Add("ginchat-local")
	deliverByType[1] = deliverPrivate
	deliverByType[2] = deliverGroup
	chatBus.Subscribe("private", savePrivate)
	chatBus.Subscribe("online", saveOnline)
	chatBus.Start()
}

func deliverPrivate(msg Message, raw []byte) {
	sendMsg(msg.TargetId, raw)
}

func deliverGroup(msg Message, raw []byte) {
	sendGroupMsg(msg.TargetId, msg.UserId, raw)
}

func nodeOf(id int64) (*Node, bool) {
	v, ok := clients.Get(id)
	if !ok || v == nil {
		return nil, false
	}
	n, ok := v.(*Node)
	return n, ok
}

func publishPrivate(key int64, body []byte) {
	err := chatBus.Publish(scale.Message{Topic: "private", Key: key, Body: body})
	if err != nil {
		log.Println("私聊队列已满，改为当前协程写入", err)
		savePrivate(scale.Message{Key: key, Body: body})
	}
}

func savePrivate(m scale.Message) {
	if utils.Red == nil {
		return
	}
	var header Message
	if err := json.Unmarshal(m.Body, &header); err != nil {
		log.Println("保存聊天记录 json", err)
		return
	}
	if header.Type != 1 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := utils.Red.ZAdd(ctx, MsgKey(header.UserId, header.TargetId), &redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: string(m.Body),
	}).Err()
	if err != nil {
		log.Println("保存聊天记录", err)
	}
}

func saveOnline(m scale.Message) {
	SetUserOnlineInfo(onlineKey(m.Key), m.Body, onlineTTL())
}

// gatewayOf 返回这个用户应当接入的机器名。单机时永远是 ginchat-local。
func gatewayOf(userId int64) string {
	return gateways.Get(strconv.FormatInt(userId, 10))
}
