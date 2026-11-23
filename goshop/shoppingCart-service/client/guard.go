package main

import (
	"errors"
	"strconv"
	"time"

	common "git.mashibing.com/msb_47094/shopping-comm"
	"git.mashibing.com/msb_47094/shopping-comm/scale"
)

// 加购网关前面的两道闸。
//
// 限流按用户 ID。一个用户把接口刷满时，其它用户的令牌桶还是满的。
// 每秒 50 次、突发 100，对人手点击足够，对脚本不够。桶的数量有上限，防止用假用户 ID 耗内存。
//
// 幂等键来自请求头 Idempotency-Key。没有这个头时和原来一样，不额外挡重复。
// 有这个头时：开始前只查询是否已经成功过；真正加购成功之后才记住。中途库存不足不会占住这个键。
// 两个完全同时的请求仍然可能都走完。要挡住那种情况，得在 Redis 里按这个键加分布式锁，单机内存做不到跨进程。
var (
	cartLimit = scale.NewLimiter(50, 100, 64, 4096)
	cartSeen  = scale.NewDedup(2 * time.Minute)
	cartChain = scale.NewChain(
		func(bag *scale.Bag) error {
			if bag == nil || !cartLimit.Allow(bag.UserID) {
				return errCartBusy
			}
			if bag.Key != "" && cartSeen.Seen(bag.Key) {
				return errCartDup
			}
			return nil
		},
	)
	errCartBusy = errors.New("请求过多，请稍后重试")
	errCartDup  = errors.New("重复提交")
)

func guardCart(userID int64, idemKey string) error {
	return cartChain.Run(&scale.Bag{UserID: userID, Key: idemKey})
}

func rememberCart(userID int32, idemKey string, skuID, number int32) {
	if idemKey != "" {
		cartSeen.First(idemKey)
	}
	common.Emit("cart", int64(userID), "sku="+strconv.FormatInt(int64(skuID), 10)+" n="+strconv.FormatInt(int64(number), 10))
}
