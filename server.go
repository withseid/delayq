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
}

func (n *Server) process(ctx context.Context, h Handler) error {
	sema := NewSemaphore(10)

	for {
		if atomic.LoadUint32(&n.close) == serverClosed {
			break
		}
		sema.Add(1)

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
				n.storage.pushToReadyQueue(h.Topic(), *job)
				return
			}

		}()
	}

	sema.Wait()
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
