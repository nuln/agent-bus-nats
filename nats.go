// Package nats implements an EventBus backed by NATS.
package nats

import (
	"context"
	"fmt"
	"os"
	"sync"

	natsgo "github.com/nats-io/nats.go"
	agent "github.com/nuln/agent-core"
)

func init() {
	agent.RegisterPluginConfigSpec(agent.PluginConfigSpec{
		PluginName:  "nats",
		PluginType:  "bus",
		Description: "NATS publish-subscribe event bus",
		Fields: []agent.ConfigField{
			{Key: "url", EnvVar: "NATS_URL", Description: "NATS server URL", Default: "nats://localhost:4222", Type: agent.ConfigFieldString},
		},
	})

	agent.RegisterEventBus("nats", func(opts map[string]any) (agent.EventBus, error) {
		url, _ := opts["url"].(string)
		if url == "" {
			url = os.Getenv("NATS_URL")
		}
		if url == "" {
			url = natsgo.DefaultURL
		}
		return New(url)
	})
}

// NATSBus implements agent.EventBus using NATS.
type NATSBus struct {
	conn *natsgo.Conn
}

// New creates a NATSBus connected to the given URL.
func New(url string) (*NATSBus, error) {
	conn, err := natsgo.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats bus: connect: %w", err)
	}
	return &NATSBus{conn: conn}, nil
}

// Publish sends payload to a topic.
func (b *NATSBus) Publish(_ context.Context, topic string, payload []byte) error {
	return b.conn.Publish(topic, payload)
}

// Subscribe registers a handler for a topic and returns a subscription.
func (b *NATSBus) Subscribe(_ context.Context, topic string, handler func([]byte)) (agent.EventSubscription, error) {
	sub, err := b.conn.Subscribe(topic, func(msg *natsgo.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("nats bus: subscribe %q: %w", topic, err)
	}
	return &natsSub{sub: sub}, nil
}

type natsSub struct {
	mu  sync.Mutex
	sub *natsgo.Subscription
}

func (s *natsSub) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}
