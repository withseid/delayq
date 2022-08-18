package main

import (
	"encoding/json"
	"time"

	"github.com/withseid/delayq"
	"github.com/withseid/delayq/example/model"
)

func main() {
	config := delayq.RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	client := delayq.NewClient(config)

	space1 := model.DeletedSpace{
		SpaceID: "space1",
	}
	data, err := json.Marshal(space1)
	if err != nil {
		panic(err)
	}

	// 假设当前时间是 2022-08-16 18：12
	// delayq.ProcessAt(time.Now().AddDate(0, 0, 1)) 表示将在 2022-08-17 18:12 执行该任务
	client.Enqueue(space1.Topic(), space1.SpaceID, data, delayq.ProcessAt(time.Now().AddDate(0, 0, 1)))

	space2 := model.DeletedSpace{
		SpaceID: "space2",
	}
	data, err = json.Marshal(space2)
	if err != nil {
		panic(err)
	}
	// 假设当前时间是 2022-08-16 18：12
	// delayq.ProcessIn(time.Hour*24) 表示将在当前时间的基础上，延迟 24 小时后执行
	client.Enqueue(space2.Topic(), space2.SpaceID, data, delayq.ProcessIn(time.Hour*24))

	// 将 JobID 为 space2 的延迟任务出队
	client.Dequeue(space2.Topic(), space2.SpaceID)
}
