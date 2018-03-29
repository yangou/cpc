package internal

import (
	"time"
)

type Option func(n *Node)

func SetLogger(logger Logger) Option {
	return Option(func(n *Node) {
		n.logger = logger
	})
}

func EnableLogger() Option {
	return Option(func(n *Node) {
		n.logger = &logger{enabled: true}
	})
}

func DisableLogger() Option {
	return Option(func(n *Node) {
		n.logger = &logger{enabled: false}
	})
}

func SetChannel(channel string) Option {
	return Option(func(n *Node) {
		n.channel = channel
	})
}

func SetWorkers(workers int) Option {
	return Option(func(n *Node) {
		n.workers = workers
	})
}

func SetLoadWaitUnit(loadWaitUnit time.Duration) Option {
	return Option(func(n *Node) {
		n.loadWaitUnit = loadWaitUnit
	})
}

func SetHeartbeatPeriod(heartbeatPeriod time.Duration) Option {
	return Option(func(n *Node) {
		n.heartbeatPeriod = heartbeatPeriod
	})
}
