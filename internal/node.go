package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/satori/go.uuid"
	"github.com/yangou/redis_lock"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Node struct {
	client          *redis.Client
	channel         string
	workers         int
	loadWaitUnit    time.Duration
	heartbeatPeriod time.Duration
	logger          Logger

	subscriber *redis.PubSub
	messages   <-chan *redis.Message
	ctx        context.Context
	cancel     context.CancelFunc

	load     int64
	handlers map[string]MessageHandler
	mux      sync.RWMutex
	wg       sync.WaitGroup
}

func (n *Node) Start() {
	n.logger.Info("cpc: starting node")

	n.load = 0
	n.handlers = map[string]MessageHandler{}
	n.wg.Add(n.workers)

	n.subscriber = n.client.PSubscribe(path.Join(n.channel, "*"))
	n.messages = n.subscriber.Channel()
	n.ctx, n.cancel = context.WithCancel(context.Background())

	for i := 0; i < n.workers; i++ {
		go n.work()
	}
}

func (n *Node) Stop() {
	n.subscriber.Close()
	n.cancel()
	n.wg.Wait()

	n.logger.Info("cpc: stopped node")
}

func (n *Node) work() {
	defer n.wg.Done()

	for {
		if msg, more := <-n.messages; more {
			n.logger.Debug("received cpc message on channel %s\n%s", msg.Channel, msg.Payload)
			topic := strings.Replace(msg.Channel, n.channel+"/", "", 1)
			if handler := n.getHandler(topic); handler != nil {
				message := &Message{}
				if err := json.Unmarshal([]byte(msg.Payload), message); err != nil {
					n.logger.Error("cpc: error unmarshalling node message, %s\n%s", err.Error(), msg.Payload)
				} else {
					n.handle(topic, message, handler)
				}
			} else {
				n.logger.Warn("cpc: received node message without handler, probably timeout message arrival")
			}
		} else {
			return
		}
	}
}

func (n *Node) getHandler(topic string) MessageHandler {
	n.mux.RLock()
	defer n.mux.RUnlock()

	return n.handlers[topic]
}

func (n *Node) putHandler(topic string, handler MessageHandler) {
	n.mux.Lock()
	defer n.mux.Unlock()

	if _, found := n.handlers[topic]; found {
		panic(fmt.Errorf("tempt to override existing handler on topic %s", topic))
	}

	n.handlers[topic] = handler
}

func (n *Node) removeHandler(topic string) {
	n.mux.Lock()
	defer n.mux.Unlock()

	delete(n.handlers, topic)
}

func (n *Node) handle(topic string, message *Message, handler MessageHandler) {
	defer func() {
		if r := recover(); r != nil {
			n.logger.Crit("cpc: panic handling node message on topic %s %s", topic, message.Dump())
		}
	}()

	if message.Type == MessageTypeResp {
		handler(n.ctx, message)
		return
	}

	load := atomic.AddInt64(&n.load, int64(message.Load)) - int64(message.Load)
	defer atomic.AddInt64(&n.load, -1*int64(message.Load))
	if load < 0 {
		n.logger.Crit("cpc: detected negative node load %d", load)
		load = 0
	}
	n.logger.Info("cpc: node current load %d, waiting for %d milliseconds to compete for message", load, int64(time.Duration(load)*n.loadWaitUnit/time.Millisecond))
	time.Sleep(time.Duration(load) * n.loadWaitUnit)

	session := redis_lock.LockSession()
	lockPeriod := 2 * n.heartbeatPeriod
	lockKey := path.Join(n.channel, "messages", message.Id)
	locked, err := redis_lock.RedisLock(n.client, lockKey, session, lockPeriod)
	if err != nil {
		n.logger.Error("cpc: error locking node message, %s", err.Error())
		return
	} else if !locked {
		n.logger.Debug("cpc: failed to lock node message, probably owned by peers")
		return
	}

	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			case <-time.After(n.heartbeatPeriod):
				extended, err := redis_lock.RedisExtendLock(n.client, lockKey, session, lockPeriod)
				if !extended {
					n.logger.Crit("cpc: error extending lock on node message, %s", err.Error())
				}
			}
		}
	}()
	defer func() {
		close(stop)
		<-stopped
	}()

	resp := handler(n.ctx, message)
	if resp.Error != "" {
		n.logger.Error("cpc: error handling node message, %s", resp.Error)
	}

	if message.Type == MessageTypeCall {
		if p, err := json.Marshal(resp); err != nil {
			n.logger.Error("cpc: error marshalling node response, %s", err.Error())
		} else {
			respTopic := path.Join(topic, message.Id)
			if err := n.client.Publish(path.Join(n.channel, respTopic), string(p)).Err(); err != nil {
				n.logger.Error("cpc: error publishing node response, %s", err.Error())
			}
		}
	}
}

func (n *Node) Cast(topic string, data []byte, load uint64) error {
	message := &Message{
		Id:        uuid.NewV4().String(),
		Timestamp: time.Now().UnixNano(),
		Type:      MessageTypeCast,
		Data:      data,
		Load:      load,
	}
	if p, err := json.Marshal(message); err != nil {
		return err
	} else {
		return n.client.Publish(path.Join(n.channel, topic), string(p)).Err()
	}
}

func (n *Node) Call(topic string, data []byte, load uint64, timeout time.Duration) ([]byte, error) {
	message := &Message{
		Id:        uuid.NewV4().String(),
		Timestamp: time.Now().UnixNano(),
		Type:      MessageTypeCall,
		Data:      data,
		Load:      load,
	}
	p, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	respCh := make(chan *Message, 1)
	respTopic := path.Join(topic, message.Id)
	n.putHandler(respTopic, MessageHandler(func(ctx context.Context, resp *Message) *Message {
		respCh <- resp
		return nil
	}))
	defer n.removeHandler(respTopic)

	if err := n.client.Publish(path.Join(n.channel, topic), string(p)).Err(); err != nil {
		return nil, err
	}

	select {
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout on cpc call on topic %s", topic)
	case resp := <-respCh:
		if resp.Error != "" {
			return resp.Data, errors.New(resp.Error)
		}
		return resp.Data, nil
	}
}

func (n *Node) Handle(topic string, fn func(context.Context, []byte) ([]byte, error)) {
	handler := MessageHandler(
		func(ctx context.Context, message *Message) *Message {
			respData, respErr := fn(ctx, message.Data)
			errMessage := ""
			if respErr != nil {
				errMessage = respErr.Error()
			}
			return &Message{
				Id:        message.Id,
				Timestamp: time.Now().UnixNano(),
				Type:      MessageTypeResp,
				Data:      respData,
				Error:     errMessage,
				Load:      0,
			}
		},
	)
	n.putHandler(topic, handler)
}

func NewNode(client *redis.Client, options ...Option) *Node {
	node := &Node{
		client:          client,
		channel:         "cpc",
		workers:         10,
		loadWaitUnit:    5 * time.Millisecond,
		heartbeatPeriod: time.Second,
		logger:          &logger{enabled: false},
	}
	for _, option := range options {
		option(node)
	}

	return node
}
