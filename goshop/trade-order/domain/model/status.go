package model

import (
	"strconv"

	"git.mashibing.com/msb_47094/shopping-comm/scale"
)

// 订单状态和课程约定一致，支付回调会把待支付改成已支付。
const (
	OrderWaitPay  int32 = 1 // 待支付
	OrderClosed   int32 = 2 // 已关闭
	OrderPaid     int32 = 3 // 已支付
	OrderShipped  int32 = 4 // 已发货
	OrderReceived int32 = 5 // 已收货
	OrderFinished int32 = 6 // 已完成
	OrderReviewed int32 = 7 // 已追评
)

// orderState 把允许的状态边收成一张表。sameOK 为 true：支付通知重放时，状态已经是目标值也算成功。
// 0 和待支付一样，可以关闭或支付。已支付之后只能往发货、收货、完成、追评走，不能从已发货退回待支付。
var orderState = func() *scale.Machine {
	m := scale.NewMachine(true)
	allow := func(from, to int32) {
		m.Allow(strconv.FormatInt(int64(from), 10), strconv.FormatInt(int64(to), 10))
	}
	allow(0, OrderClosed)
	allow(0, OrderPaid)
	allow(OrderWaitPay, OrderClosed)
	allow(OrderWaitPay, OrderPaid)
	allow(OrderPaid, OrderShipped)
	allow(OrderPaid, OrderClosed)
	allow(OrderShipped, OrderReceived)
	allow(OrderReceived, OrderFinished)
	allow(OrderFinished, OrderReviewed)
	return m
}()

// CanTransit 判断订单能不能从 from 变成 to。规则在 orderState 里，不在调用方的 if 里。
func CanTransit(from, to int32) bool {
	return orderState.Can(strconv.FormatInt(int64(from), 10), strconv.FormatInt(int64(to), 10))
}
