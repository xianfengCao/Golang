package biz

import "laomadj/scale"

// driverState 是司机状态机。相同状态允许，重复提交审核或听单不会报错。
// 新登记是 stop。只能先到 in（收车），不能从停用直接出车。
// 听单和出车可以来回，停用可以从任意工作状态进入。
var driverState = scale.NewMachine(true).
	Allow(DriverStatusStop, DriverStatusIn).
	Allow(DriverStatusIn, DriverStatusListen).
	Allow(DriverStatusListen, DriverStatusOut).
	Allow(DriverStatusOut, DriverStatusListen).
	Allow(DriverStatusListen, DriverStatusIn).
	Allow(DriverStatusIn, DriverStatusStop).
	Allow(DriverStatusListen, DriverStatusStop).
	Allow(DriverStatusOut, DriverStatusStop)

// CanTransitDriver 只认 driverState 里的边。
func CanTransitDriver(from, to string) bool {
	return driverState.Can(from, to)
}
