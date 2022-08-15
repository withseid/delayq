package delayq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

type Space struct {
	ID     string
	UserID string
	Phone  string
}

var spaceExpiredTopic = "space_expired"

func TestNewClient(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	client := NewClient(config)
	space := Space{
		ID:     "space2",
		UserID: "user2",
		Phone:  "phone2",
	}

	data, err := json.Marshal(space)
	if err != nil {
		panic(err)
	}

	topic := "space_expired"
	for i := 0; i < 100; i++ {
		client.Enqueue(topic, fmt.Sprintf("job_%s", uuid.NewV4().String()), data, ProcessAt(time.Now().Add(time.Second)))
	}

}
