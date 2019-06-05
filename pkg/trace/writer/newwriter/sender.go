package writer

import (
	"container/list"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/DataDog/datadog-agent/pkg/util/log"
)

var errRetryLater = errors.New("request can not be completed, try again later")

type payload struct {
	header map[string]string
	body   io.Reader
}

func newPayload(body io.Reader, headers map[string]string) *payload {
	p := payload{
		body:   body,
		header: headers,
	}
	return &p
}

type sender struct {
	client *http.Client
	url    string

	mu         sync.RWMutex // guards below group
	retryQueue *list.List   // *http.Request's queued for retrying
	retry      int          // next retry number (0=off, 1=first)
}

func newSender(client *http.Client, url string) *sender {
	s := sender{
		url:        url,
		client:     client,
		retryQueue: list.New(),
	}
	return &s
}

func (s *sender) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	done := make(chan *list.Element, s.retryQueue.Len())
	ctx, cancel := context.WithCancel(context.Background())

	for e := s.retryQueue.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		// we try flushing payloads concurrently
		go func(e *list.Element) {
			defer wg.Done()
			req := e.Value.(*http.Request)
			switch err := s.do(req.WithContext(ctx)); err {
			case nil:
				done <- e
			case errRetryLater:
				// cancel any pending requests, we need to start backing off
				cancel()
			default:
				if uerr := err.(*url.Error); uerr.Err == context.Canceled {
					// canceled, will try again next time
					return
				}
				done <- e
				log.Errorf("Error sending payload: %v", err)
			}
		}(e)
	}

	wg.Wait()
outer:
	for {
		select {
		case e := <-done:
			s.retryQueue.Remove(e)
		default:
			break outer
		}
	}
	switch s.retryQueue.Len() {
	case 0:
		// everything was sent
		s.retry = 0
	default:
		// we need to continue backing off
		s.retry++
		defer time.AfterFunc(fullJitterDuration(s.retry), s.flush)
	}
}

func (s *sender) send(p *payload) {
	req, err := http.NewRequest(http.MethodPost, s.url, p.body)
	if err != nil {
		// this should never happen with sanitized data (invalid method or invalid url)
		log.Errorf("http.NewRequest: %s", err)
		return
	}
	for k, v := range p.header {
		req.Header.Add(k, v)
	}
	s.mu.RLock()
	backoff := s.retry > 0
	s.mu.RUnlock()
	if backoff {
		// we are backing off for now, push into the queue
		s.mu.Lock()
		s.retryQueue.PushBack(req)
		s.mu.Unlock()
		return
	}
	go func() {
		switch err := s.do(req); err {
		case nil:
			return
		case errRetryLater:
			s.mu.Lock()
			s.retryQueue.PushBack(req)
			if s.retry == 0 {
				// this is the first retriable failure, schedule a retry
				s.retry++
				time.AfterFunc(fullJitterDuration(s.retry), s.flush)
			}
			s.mu.Unlock()
		default:
			log.Errorf("Error sending payload: %s", err)
		}
	}()
}

func (s *sender) do(req *http.Request) error {
	resp, err := s.client.Do(req)
	if err != nil {
		// request errors are either redirect errors or url errors
		return errRetryLater
	}
	if resp.StatusCode/100 == 5 {
		// 5xx errors can be retried
		return errRetryLater
	}
	if resp.StatusCode/100 != 2 {
		// non-2xx errors are failures
		return err
	}
	return nil
}

const (
	base        = 200 * time.Millisecond
	maxDuration = 30 * time.Second
)

// fullJitter implements Full Jitter Backoff as per https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func fullJitterDuration(n int) time.Duration {
	pow := math.Min(math.Pow(2, float64(n)), math.MaxInt64)
	ns := math.Min(float64(base)*pow, math.MaxInt64)
	if ns > float64(maxDuration) {
		return maxDuration
	}
	return time.Duration(ns)
}
