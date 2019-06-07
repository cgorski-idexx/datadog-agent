package writer

/*

import (
	"bytes"
	"container/list"
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/DataDog/datadog-agent/pkg/util/log"
)

type sender struct {
	client *http.Client
	url    string

	mu         sync.RWMutex // guards below group
	retryQueue *list.List   // *http.Request's queued for retrying
	attempt    int          // next attempt number (0=off, 1=first)
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
		s.attempt = 0
	default:
		// we need to continue backing off
		s.attempt++
		defer time.AfterFunc(fullJitterDuration(s.attempt), s.flush)
	}
}

func (s *sender) send(p *payload) {
	req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(p.body))
	if err != nil {
		// this should never happen with sanitized data (invalid method or invalid url)
		log.Errorf("http.NewRequest: %s", err)
		return
	}
	for k, v := range p.headers {
		req.Header.Add(k, v)
	}

	// check if we are in backoff state
	s.mu.RLock()
	backoff := s.attempt > 0
	s.mu.RUnlock()
	if backoff {
		// we seem to be backing off
		s.mu.Lock()
		if s.attempt > 0 {
			s.retryQueue.PushBack(req)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}

	// go ahead and flush asynchronously
	go func() {
		switch err := s.do(req); err {
		case nil:
			return
		case errRetryLater:
			s.mu.Lock()
			s.retryQueue.PushBack(req)
			if s.attempt == 0 {
				// this is the first retriable failure, schedule a retry
				s.attempt++
				time.AfterFunc(fullJitterDuration(s.attempt), s.flush)
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
*/
