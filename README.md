# 延迟队列 delayq



## 业务背景

在工作时，遇到以下业务需求

- 某个资源过期后，在过期后的第 30 天后会自动删除，但是在自动删除前，需要在在过期第 23 天，28天，29天发短息通知用户，该过期资源将被删除

- 用户要删除某个重要资源时，需要留 24 小时的缓冲时间给用户考虑是否真的要删掉，若 24 小时内用户没有撤销删除操作，则 24 小时后将该资源删除

怎么去解决以上的业务需求呢？ 一开始最容易想到的就是后台一直去轮询 mysql 的表，看看是否该删除空间/发短信通知用户。这种做法存在的问题是： mysql 的性能比较容易到达瓶颈，一直去扫表，mysql 的压力会很大。因此这个方案是不可行的。



后来，我们采用的方案是**延迟队列**，一开始考虑的是用 [beanstalkd](https://github.com/beanstalkd/beanstalkd) , 但是 beanstalkd 存在一个问题是，只支持入队操作，不支持出队操作。因此 beanstalkd 也被抛弃了。因为在我们的需求中，是需要有出队操作的。

例如，当用户删除某个资源时，将该准备删除的资源加入延迟队列，24小时后自动删除该资源，如果在 24 小时内，用户点击了撤销操作，此时需要将该资源的信息从延迟队列中删除。



后面参考了[有赞的延迟队列设计](https://tech.youzan.com/queuing_delay/) 的文章，实现了一个延迟队列。在延迟队列设计中，最重要的就是以下三个组件

- Job Pool：存放 Job 的元信息
- Delay Queue： 存放暂不可被消费的 jobID
- Ready Queue： 存放着可以被消费的 jobID

![](https://s3.bmp.ovh/imgs/2022/08/16/d64403a68669df2f.png)

将一个延迟任务入队的流程如下： 
- 将 Job 的元信息添加到 Job Pool 中
- 将 Job ID 和 到期时间存入到 Delay Queue
- Timer 每隔 1 秒钟去扫描 Delay Queue ，如果发现当前时间大于等于某个 Job 的到期时间，将 Job ID 添加到 Ready Queue 中，并且把 Delay Queue 中的对应的 Job ID 删除
- Ready Queue 中的 Job ID 会被消费者消费掉

将一个非延迟任务队列入队流程如下：
- 将 Job 的元信息添加到 Job Pool 中
- 将 Job ID 存入到 Ready Queue
- Ready Queue 中的 Job ID 会被消费者消费掉

将一个延迟队列出队流程如下：
- 根据 JobID 将 Delay Queue 中对应的数据删掉
- 根据 JobID 将 Job Pool 中的元信息删除

## Quickstart 
通过 go get 安装 delayq 
```go 
go get -u github.com/withseid/delayq
```

初始化客户端
```go
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




}
```

延迟任务入队可选 Option 
- ProcessAt: 在某个时间点执行任务
- ProcessIn: 从当前时间算起，延迟多久后再执行任务
- Retry：重试次数，此 Option 不配置时，默认无限重试，重试时间每次递增，从 2<sup>1</sup> -> 2<sup>12 </sup> s

在可选 Option 中，若 ProcessAt 和 ProcessIn 都不选，则该任务立即执行。若 ProcessAt 和 ProcessIn 都选了，则只有最后一个会生效。 

可选 Option ProcessAt, 例如 `delayq.ProcessAt(time.Now().AddDate(0, 0, 1))` 表示将在第二天的这个时间执行某个任务
```go
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
```

可选 Option ProcessIn，例如 `delayq.ProcessIn(time.Second*10)` 表示 5s 后执行某个任务，
``` go
	space2 := model.DeletedSpace{
		SpaceID: "space2",
	}
	data, err := json.Marshal(space2)
	if err != nil {
		panic(err)
	}
	// delayq.ProcessIn(time.Second*24) 表示将在当前时间的基础上，延迟 10s 后执行
	client.Enqueue(space2.Topic(), space2.SpaceID, data, delayq.ProcessIn(time.Second*10))
```

可选 Option Retry， 例如 `delayq.Retry(6)`, 表示重试 6 次，若重试 6 次都失败，则自动丢弃任务，默认为无限重试
```go 
	space3 := model.DeletedSpace{
		SpaceID: "space3",
	}
	data, err := json.Marshal(space3)
	if err != nil {
		panic(err)
	}
	client.Enqueue(space3.Topic(), space3.SpaceID, data, delayq.Retry(6))
```

出队
``` go
	// 将 JobID 为 space2 的延迟任务出队
	client.Dequeue(space2.Topic(), space2.SpaceID)
```



新建一个 Server，负责消费延迟队列中的消息并且执行延迟延迟 
``` go
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
```

DeletedSpace 类型的延迟任务的处理逻辑 
```go
package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/withseid/delayq"
)

// const DeletedSpaceTopic = "deleted_space"

type DeletedSpace struct {
	SpaceID string
}

func (d *DeletedSpace) Topic() string {
	return "deleted_space"
}

func (d *DeletedSpace) Execute(ctx context.Context, job *delayq.Job) error {
	ds := DeletedSpace{}
	err := json.Unmarshal(job.Boday, &ds)
	if err != nil {
		return err
	}

	fmt.Println("DeletedSpace: ", ds)
	return nil
}
```
