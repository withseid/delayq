package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

func TestMigrate(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	cli, err := initRedis(config)
	if err != nil {
		t.Fatal(err)
	}
	readyQueueKey := fmt.Sprintf("%s_space_expired", RedisReadyQueue)
	ti := time.Now().Unix()

	jobIDs, err := migrateExpiredJobScript.Run(context.TODO(), cli,
		[]string{"delayQ_delay_queue_space_expired", readyQueueKey},
		[]interface{}{ti}).StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("jobIDs size: ", len(jobIDs))
	fmt.Println("jobIDs: ", jobIDs)
}

func TestTime(t *testing.T) {
	now := time.Now()
	fmt.Println(now.Unix())
}

func TestDelayQServer(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}

	// space := Space{}

	s := NewServer(config)

	s.Run(context.Background())
}

var spaceExpiredTopic = "space_expired"

type Space struct {
	ID     string
	UserID string
	Phone  string
}

func (s *Space) Topic() string {
	return spaceExpiredTopic
}

func (s *Space) Execute(ctx context.Context, payload []byte) error {

	space := Space{}
	err := json.Unmarshal(payload, &space)
	if err != nil {
		return err
	}

	fmt.Println("[Execute] Space: ", space)
	return nil
}

func TestNewClient(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	client := NewClient(config)

	rand.Seed(time.Now().UnixNano())
	topic := "space_expired"
	for i := 0; i < 5000; i++ {
		space := Space{
			ID:     fmt.Sprintf("space%d", i),
			UserID: fmt.Sprintf("user%d", i),
			Phone:  fmt.Sprintf("phone%d", i),
		}

		data, err := json.Marshal(space)
		if err != nil {
			panic(err)
		}
		t := rand.Int31n(300)
		fmt.Println(t)
		client.Enqueue(topic, fmt.Sprintf("job_%s", uuid.NewV4().String()), data, ProcessAt(time.Now().Add(time.Minute*time.Duration(t))))
	}

}
