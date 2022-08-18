package main

import (
	"context"

	"github.com/withseid/delayq"
	"github.com/withseid/delayq/example/model"
)

func main() {
	config := delayq.RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	server := delayq.NewServer(config)
	ds := model.DeletedSpace{}

	server.HandlerFunc(ds.Topic(), &ds)
	server.Run(context.TODO())
}
