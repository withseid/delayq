package delayq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)




func TestNewClient(t *testing.T) {
	config := RedisConfiguration{
		Host: "127.0.0.1",
		Port: "6379",
	}
	client := NewClient(config)

	topic := "space_expired"
	for i := 0; i < 100; i++ {
		space := Space{
			ID:     fmt.Sprintf("space%d", i),
			UserID: fmt.Sprintf("user%d", i),
			Phone:  fmt.Sprintf("phone%d", i),
		}

		data, err := json.Marshal(space)
		if err != nil {
			panic(err)
		}
		client.Enqueue(topic, fmt.Sprintf("job_%s", uuid.NewV4().String()), data, ProcessAt(time.Now().Add(time.Second)))
	}
	// client.Enqueue(topic, fmt.Sprintf("job_%s", uuid.NewV4().String()), data, ProcessAt(time.Now().Add(time.Hour)))
}
