package cpc

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/yangou/cpc/internal"
	"time"
)

var SetLogger = internal.SetLogger
var EnableLogger = internal.EnableLogger
var DisableLogger = internal.DisableLogger
var SetChannel = internal.SetChannel
var SetWorkers = internal.SetWorkers
var SetloadWaitUnit = internal.SetloadWaitUnit
var SetHeartbeatPeriod = internal.SetHeartbeatPeriod

func NewNode(client *redis.Client, options ...internal.Option) *internal.Node {
	return internal.NewNode(client, options...)
}

var node *internal.Node

func Start(client *redis.Client, options ...internal.Option) {
	node = NewNode(client, options...)
	node.Start()
}

func Stop() { node.Stop() }

func Cast(topic string, data []byte, load uint64) error {
	return node.Cast(topic, data, load)
}

func CastWithKey(topic, key string, data []byte, load uint64) error {
	return node.CastWithKey(topic, key, data, load)
}

func Call(topic string, data []byte, load uint64, timeout time.Duration) ([]byte, error) {
	return node.Call(topic, data, load, timeout)
}

func CallWithKey(topic, key string, data []byte, load uint64, timeout time.Duration) ([]byte, error) {
	return node.CallWithKey(topic, key, data, load, timeout)
}

func Handle(topic string, fn func(context.Context, []byte) ([]byte, error)) { node.Handle(topic, fn) }
