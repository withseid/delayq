package delayq

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
)

type Server struct {
	stopCh   chan struct{}
	close    uint32
	storage  storage
	handlers map[string]Handler
}

func NewServer(config RedisConfiguration) *Server {
	storage, err := newStorage(config)
	log.Println("Init Storage")
	if err != nil {
		log.Fatalf("[delayq error] newStorage error: %+v\n", err)
	}

	s := Server{
		stopCh:   make(chan struct{}),
		close:    0,
		storage:  storage,
		handlers: make(map[string]Handler),
	}
	return &s
}

func (n *Server) Run(ctx context.Context) error {
	for topic, h := range n.handlers {
		go n.migrateExpiredJob(topic)
		go n.process(ctx, h)
	}

	go n.watchSystemSignal(ctx)
	<-n.stopCh
	return nil
}

func (s *Server) watchSystemSignal(ctx context.Context) {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	s.stopCh <- struct{}{}
	atomic.AddUint32(&s.close, 1)
}

func (s *Server) migrateExpiredJob(topic string) {

	log.Println("migrateExpiredJob start")
	ticker := time.NewTicker(time.Duration(time.Second * 1))
	for {
		if atomic.LoadUint32(&s.close) == serverClosed {
			break
		}
		select {
		case <-ticker.C:
			s.storage.migrateExpiredJob(topic)
		}
	}
	log.Println("migrateExpiredJob end")
}

func (n *Server) process(ctx context.Context, h Handler) error {
	log.Println("process start")
	sema := NewSemaphore(10)

	// 启动时，会先将上次 process 没处理完成的数据迁移到 ready queue 中，等待重新执行
	err := n.storage.unfinishToReady(h.Topic())
	if err != nil {
		log.Print(err)
		return err
	}
	for {
		if atomic.LoadUint32(&n.close) == serverClosed {
			break
		}
		sema.Add(1)
		// getReadyJob 的同时，将 job 存放到 process 中
		job, err := n.storage.getReadyJob(h.Topic())
		if err != nil && err != redis.Nil {
			sema.Done()
			continue
		}

		if job == nil {
			sema.Done()
			time.Sleep(time.Second * 1)
			continue
		}
		go func() {
			defer sema.Done()
			err := h.Execute(ctx, job)
			if err != nil {
				log.Println("[Execute] Error: ", err)

				var retryInterval int64

				job.RetryCount++

				// 有限重试且重试次数到了， return
				if job.MaxRetry != -1 && job.RetryCount >= job.MaxRetry {
					return
				}

				if job.RetryCount >= 12 {
					retryInterval = 1 << 12
				} else {
					retryInterval = 1 << job.RetryCount
				}
				job.Delay = time.Now().Add(time.Duration(retryInterval) * time.Second).Unix()
				n.storage.pushToDelayQueue(h.Topic(), *job)
				return
			}

			// 完成后，delete job
			err = n.storage.deleteFinishJob(h.Topic(), job.ID)
			if err != nil {
				log.Println("[Execute] Error: ", err)
			}

		}()
	}

	sema.Wait()
	log.Println("process end")
	return nil
}

func (n *Server) HandlerFunc(topic string, handler Handler) {
	if handler == nil {
		panic("[delayq error] nil handler")
	}
	n.handlers[topic] = handler
}

type Handler interface {
	Execute(ctx context.Context, job *Job) error
	Topic() string
}
