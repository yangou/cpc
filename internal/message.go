package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type MessageType string

const (
	MessageTypeCast MessageType = "cast"
	MessageTypeCall MessageType = "call"
	MessageTypeResp MessageType = "resp"
)

func (t MessageType) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(t))
}

func (t *MessageType) UnmarshalJSON(b []byte) error {
	var _t string
	err := json.Unmarshal(b, &_t)
	if err != nil {
		return err
	}
	switch MessageType(_t) {
	case MessageTypeCast:
		*t = MessageTypeCast
	case MessageTypeCall:
		*t = MessageTypeCall
	case MessageTypeResp:
		*t = MessageTypeResp
	default:
		return fmt.Errorf("invalid message type %s", _t)
	}
	return nil
}

type Message struct {
	Id        string      `json:"id"`
	Key       string      `json:"key"`
	Timestamp int64       `json:"timestamp"`
	Type      MessageType `json:"type"`
	Data      []byte      `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	Load      uint64      `json:"load"`
}

func (m *Message) Dump() string {
	return fmt.Sprintf(`
id: %s
timestamp: %s
type: %s
data: %s
error: %s
load: %d
`, m.Id, time.Unix(0, m.Timestamp).Format(time.RFC3339Nano), string(m.Type), string(m.Data), m.Error, m.Load)
}

type MessageHandler func(context.Context, *Message) *Message
