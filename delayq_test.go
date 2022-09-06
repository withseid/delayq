package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestDelayQueue(t *testing.T) {

	go producer()

	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	server := NewServer(config)
	ds := DeletedSpace{}

	server.HandlerFunc(ds.Topic(), &ds)
	server.Run(context.TODO())

}

func producer() {

	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	client := NewClient(config)

	space1 := DeletedSpace{
		SpaceID: "space1",
	}
	data, err := json.Marshal(space1)
	if err != nil {
		panic(err)
	}

	// 假设当前时间是 2022-08-16 18：12
	// delayq.ProcessAt(time.Now().AddDate(0, 0, 1)) 表示将在 2022-08-17 18:12 执行该任务
	client.Enqueue(space1.Topic(), space1.SpaceID, data, ProcessAt(time.Now().AddDate(0, 0, 1)))

	space2 := DeletedSpace{
		SpaceID: "space2",
	}
	data, err = json.Marshal(space2)
	if err != nil {
		panic(err)
	}
	// 假设当前时间是 2022-08-16 18：12
	// delayq.ProcessIn(time.Hour*24) 表示将在当前时间的基础上，延迟 24 小时后执行
	client.Enqueue(space2.Topic(), space2.SpaceID, data, ProcessIn(time.Hour*24))
}

// const DeletedSpaceTopic = "deleted_space"

type DeletedSpace struct {
	SpaceID string
}

func (d *DeletedSpace) Topic() string {
	return "deleted_space"
}

func (d *DeletedSpace) Execute(ctx context.Context, job *Job) error {
	ds := DeletedSpace{}
	err := json.Unmarshal(job.Boday, &ds)
	if err != nil {
		return err
	}
	fmt.Println("DeletedSpace job info: ", job)

	return nil
}
