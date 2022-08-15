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
	"golang.org/x/sync/semaphore"
)

type server struct {
	stopCh  chan struct{}
	close   uint32
	workers []*Worker
	storage storage
}

type Worker struct {
	TopicName   string
	Handler     JobHandler
	Concurrency int
	WorkerPool  semaphore.Weighted
}

func (s *server) migrateExpiredJob(topic string) {
	ticker := time.NewTicker(time.Duration(time.Second * 1))
	for {

		select {
		case <-ticker.C:
			s.storage.migrateExpiredJob(topic)
		}

	}
}

func NewServer(config RedisConfiguration, workers []*Worker) *server {

	storage, err := newStorage(config)
	if err != nil {
		log.Fatal(err)
	}

	s := server{
		stopCh:  make(chan struct{}),
		close:   0,
		storage: storage,
		workers: workers,
	}
	return &s
}

type JobHandler interface {
	Topic() string
	Execute(context.Context, []byte) error
}

func (s *server) Run(ctx context.Context) error {

	for _, worker := range s.workers {
		go s.migrateExpiredJob(worker.TopicName)
		go s.process(ctx, worker)
	}

	go s.watchSystemSignal(ctx)

	<-s.stopCh
	return nil
}

func (s *server) watchSystemSignal(ctx context.Context) {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	s.stopCh <- struct{}{}
}

func (s *server) process(ctx context.Context, worker *Worker) error {

	for {
		if atomic.LoadUint32(&s.close) == serverClosed {
			break
		}
		if err := worker.WorkerPool.Acquire(ctx, 1); err != nil {
			return err
		}

		job, err := s.storage.getReadyJob(worker.TopicName)
		if err != nil && err != redis.Nil {
			worker.WorkerPool.Release(1)
			continue
		}

		if job == nil {
			worker.WorkerPool.Release(1)
			time.Sleep(time.Second * 1)
			continue
		}

		go func() {
			err := worker.Handler.Execute(ctx, job.Boday)
			if err != nil {
				// TODO: 将该任务放回 ReadyQueue, 并且要返回错误

				return
			}
			worker.WorkerPool.Release(1)
		}()
	}

	if err := worker.WorkerPool.Acquire(ctx, int64(worker.Concurrency)); err != nil {
		return err
	}

	return nil
}
