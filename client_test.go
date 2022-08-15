package delayq

import (
	"encoding/json"
	"testing"
	"time"
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
		ID:     "space1",
		UserID: "user1",
		Phone:  "phone1",
	}

	data, err := json.Marshal(space)
	if err != nil {
		panic(err)
	}

	topic := "space_expired"
	client.Enqueue(topic, "job_id1", data, ProcessAt(time.Now().AddDate(1, 0, 0)))

	space.ID = "space2"
	space.UserID = "user2"
	space.Phone = "phone2"
	data, err = json.Marshal(space)
	if err != nil {
		panic(err)
	}
	client.Enqueue(topic, "job_id2", data, ProcessIn(time.Hour*2))

	space.ID = "space3"
	space.UserID = "user3"
	space.Phone = "phone3"
	data, err = json.Marshal(space)
	if err != nil {
		panic(err)
	}
	client.Enqueue(topic, "job_id3", data)

}
