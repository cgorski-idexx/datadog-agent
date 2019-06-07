package writer

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reaction specifies a response reaction that the test server may have.
type reaction int

const (
	reactionFail   = 0 // response will be 4xx
	reactionRetry  = 1 // response will be 5xx
	reactionAccept = 2 // response will be 2xx
)

// payloadWithReaction creates a new payload for the test server. The test server will
// respond with the given reactions, in the given order, for each subsequent request.
func payloadWithReaction(reactions ...reaction) *payload {
	var str bytes.Buffer
	str.WriteString(strconv.FormatInt(time.Now().Unix(), 10))
	str.WriteString("|")
	for _, r := range reactions {
		str.WriteString(strconv.Itoa(int(r)))
	}
	return newPayload(str.Bytes(), nil)
}

type testServer struct {
	t      *testing.T
	URL    string
	server *httptest.Server

	total    uint64
	retried  uint64
	failed   uint64
	accepted uint64

	mu   sync.Mutex
	seen map[string]*requestStatus
}

type requestStatus struct {
	count     int
	responses []reaction
}

func (ts *testServer) Failed() int   { return int(atomic.LoadUint64(&ts.failed)) }
func (ts *testServer) Retried() int  { return int(atomic.LoadUint64(&ts.retried)) }
func (ts *testServer) Total() int    { return int(atomic.LoadUint64(&ts.total)) }
func (ts *testServer) Accepted() int { return int(atomic.LoadUint64(&ts.accepted)) }

// ServeHTTP responds based on the request body.
//  - Request body "retry" results in 504
//  - Request body "accept" results in 200
//  - Request body "fail" results in 400
func (ts *testServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	atomic.AddUint64(&ts.total, 1)
	if req.Header.Get("DD-Api-Key") == "" {
		ts.t.Fatal("API key not in request headers")
	}
	v := req.Header.Get("Content-Length")
	slurp, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ts.t.Fatal(err)
	}
	if res, err := strconv.Atoi(v); err != nil || res != len(slurp) {
		ts.t.Fatalf("Content-Length header invalid: %v (%d/%d)", err, res, len(slurp))
	}

	parts := strings.Split(string(slurp), "|")
	if len(parts) != 2 {
		ts.t.Fatal("invalid payload body, please use payloadWithReaction in tests")
	}
	id, rstr := parts[0], parts[1]
	ts.mu.Lock()
	defer ts.mu.Unlock()
	p, ok := ts.seen[id]
	if !ok {
		ts.seen[id] = &requestStatus{responses: make([]reaction, len(rstr))}
		p = ts.seen[id]
		for i, ch := range rstr {
			r, err := strconv.Atoi(string(ch))
			if err != nil {
				ts.t.Fatal(err)
			}
			p.responses[i] = reaction(r)
		}
	}
	switch p.responses[p.count%len(p.responses)] {
	case reactionRetry:
		w.WriteHeader(http.StatusGatewayTimeout)
		atomic.AddUint64(&ts.retried, 1)
	case reactionAccept:
		w.WriteHeader(http.StatusOK)
		atomic.AddUint64(&ts.accepted, 1)
	case reactionFail:
		w.WriteHeader(http.StatusBadRequest)
		atomic.AddUint64(&ts.failed, 1)
	}
	p.count++
}

func (ts *testServer) Close() { ts.server.Close() }

func newTestServer(t *testing.T) *testServer {
	srv := &testServer{
		t:    t,
		seen: make(map[string]*requestStatus),
	}
	srv.server = httptest.NewServer(srv)
	srv.URL = srv.server.URL
	return srv
}

func useBackoffDuration(d time.Duration) func() {
	old := backoffDuration
	backoffDuration = func(attempt int) time.Duration { return d }
	return func() {
		backoffDuration = old
	}
}
