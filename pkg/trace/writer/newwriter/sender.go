package writer

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/info"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

var errRetryLater = errors.New("request can not be completed, try again later")

type payload struct {
	body    []byte
	headers map[string]string
}

func (p *payload) httpRequest(url string) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(p.body))
	if err != nil {
		// this should never happen with sanitized data (invalid method or invalid url)
		return nil, err
	}
	for k, v := range p.headers {
		req.Header.Add(k, v)
	}
	return req, nil
}

func newPayload(body []byte, headers map[string]string) *payload {
	p := payload{
		body:    body,
		headers: headers,
	}
	if p.headers == nil {
		p.headers = make(map[string]string, 1)
	}
	p.headers["Content-Length"] = strconv.Itoa(len(body))
	return &p
}

var maxQueueSize = 64 * 1024 * 1024 // 64MB; replaced in tests

type sender struct {
	client *http.Client
	url    string
	apiKey string
	wg     sync.WaitGroup
	sema   chan struct{} // semaphore for limiting goroutines

	mu        sync.Mutex // guards below fields
	list      *list.List // send queue
	size      int        // size of send queue
	attempt   int        // retry attempt following
	scheduled bool       // flush scheduled; TODO(gbbr): scheduled == q.list.Len() > 0 ? if yes, remove it
	timer     *time.Timer
}

func newSender(client *http.Client, url, apiKey string) *sender {
	return &sender{
		client: client,
		url:    url,
		sema:   make(chan struct{}, 200),
		list:   list.New(),
		apiKey: apiKey,
	}
}

func (q *sender) Push(p *payload) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.enqueueLocked(p)
	if !q.scheduled {
		q.sema <- struct{}{}
		q.scheduled = true
		q.wg.Add(1)
		go func() {
			q.flush()
			<-q.sema
		}()
	}
}

func (q *sender) drainQueue() []*payload {
	q.mu.Lock()
	defer q.mu.Unlock()
	var payloads []*payload
	for q.list.Len() > 0 {
		v := q.list.Remove(q.list.Front())
		payloads = append(payloads, v.(*payload))
	}
	q.size = 0
	return payloads
}

func (q *sender) sendQueue() (failed uint64) {
	var wg sync.WaitGroup
	payloads := q.drainQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // avoid leaks
	for _, p := range payloads {
		q.sema <- struct{}{}
		wg.Add(1)
		go func(p *payload) {
			defer wg.Done()
			defer func() { <-q.sema }()
			req, err := p.httpRequest(q.url)
			if err != nil {
				log.Errorf("Error creating http.Request: %s", err)
				return
			}
			switch err := q.do(req.WithContext(ctx)); err {
			case nil:
				// OK
			case errRetryLater:
				// postpone any requests in progress and try again later
				cancel()
				q.enqueue(p)
				atomic.AddUint64(&failed, 1)
			default:
				if uerr, ok := err.(*url.Error); ok && uerr.Err == context.Canceled {
					// postponed
					q.enqueue(p)
				} else {
					// not retriable
					log.Errorf("Error sending payload: %v", err)
				}
			}
		}(p)
	}
	wg.Wait()
	return failed
}

func (q *sender) flush() {
	defer q.wg.Done()
	failed := q.sendQueue()
	q.mu.Lock()
	defer q.mu.Unlock()
	if failed > 0 {
		// some requests failed, backoff
		q.attempt++
		q.scheduled = true
		q.timer = time.AfterFunc(backoffDuration(q.attempt), func() {
			q.wg.Add(1)
			q.flush()
		})
		return
	}
	// all succeeded
	q.attempt = 0
	q.scheduled = false
	if q.list.Len() > 0 {
		// some items came in while flushing
		q.scheduled = true
		q.wg.Add(1)
		q.sema <- struct{}{}
		go func() {
			q.flush()
			<-q.sema
		}()
	}
}

func (q *sender) Flush() {
	q.mu.Lock()
	if q.timer != nil {
		fmt.Println(q.timer.Stop())
	}
	q.mu.Unlock()
	q.wg.Wait()
	q.sendQueue()
}

func (q *sender) enqueue(p *payload) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueueLocked(p)
}

func (q *sender) enqueueLocked(p *payload) {
	size := len(p.body)
	for q.size+size > maxQueueSize {
		// make room
		v := q.list.Remove(q.list.Front())
		q.size -= len(v.(*payload).body)
		// TODO: log and metric
	}
	q.list.PushBack(p)
	q.size += size
}

// userAgent is the computed user agent we'll use when communicating with Datadog
var userAgent = fmt.Sprintf("Datadog Trace Agent/%s/%s", info.Version, info.GitCommit)

func (q *sender) do(req *http.Request) error {
	req.Header.Set("DD-Api-Key", q.apiKey)
	req.Header.Set("User-Agent", userAgent)
	resp, err := q.client.Do(req)
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
	base        = 100 * time.Millisecond
	maxDuration = 10 * time.Second
)

// backoffDuration returns the backoff duration necessary for the given attempt.
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
var backoffDuration = func(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}
	pow := math.Min(math.Pow(2, float64(attempt)), math.MaxInt64)
	ns := int64(math.Min(float64(base)*pow, math.MaxInt64))
	if ns > int64(maxDuration) {
		ns = int64(maxDuration)
	}
	return time.Duration(rand.Int63n(ns))
}
