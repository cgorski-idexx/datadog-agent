package writer

import (
	"bytes"
	"compress/gzip"
	"strings"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/config"
	"github.com/DataDog/datadog-agent/pkg/trace/info"
	"github.com/DataDog/datadog-agent/pkg/trace/metrics/timing"
	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/DataDog/datadog-agent/pkg/trace/traceutil"
	"github.com/DataDog/datadog-agent/pkg/util/log"

	"github.com/gogo/protobuf/proto"
)

const pathTraces = "/api/v0.2/traces"

// payloadFlushThreshold specifies the maximum accumulated payload size that is allowed before
// a flush is triggered; replaced in tests.
var payloadFlushThreshold = 3200000 // 3.2MB is the maximum allowed by the Datadog API

// SampledSpans represents the result of a trace sampling operation.
type SampledSpans struct {
	// Trace will contain a trace if it was sampled or be empty if it wasn't.
	Trace pb.Trace
	// Events contains all APM events extracted from a trace. If no events were extracted, it will be empty.
	Events []*pb.Span
}

// Empty returns true if this TracePackage has no data.
func (ss *SampledSpans) Empty() bool {
	return len(ss.Trace) == 0 && len(ss.Events) == 0
}

// size returns the estimated size of the package.
func (ss *SampledSpans) size() int {
	// we use msgpack's Msgsize() heuristic because it is a good indication
	// of the weight of a span and the msgpack size is relatively close to
	// the protobuf size, which is expensive to compute.
	return ss.Trace.Msgsize() + pb.Trace(ss.Events).Msgsize()
}

type TraceWriter struct {
	in       <-chan *SampledSpans
	hostname string
	env      string
	senders  []sender

	traces       []*pb.APITrace // traces buffered
	events       []*pb.Span     // events buffered
	bufferedSize int            // estimated buffer size
}

func NewTraceWriter(conf *config.AgentConfig, in <-chan *SampledSpans) *TraceWriter {
	return &TraceWriter{
		in:       in,
		hostname: conf.Hostname,
		env:      conf.DefaultEnv,
	}
}

func (w *TraceWriter) Run() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case pkg, ok := <-w.in:
			if !ok {
				return
			}
			if pkg.Empty() {
				return
			}
			w.addSpans(pkg)
		case <-t.C:
			w.flush()
		}
	}
}

func (w *TraceWriter) addSpans(pkg *SampledSpans) {
	size := pkg.size()
	if size+w.bufferedSize > payloadFlushThreshold {
		// reached maximum allowed buffered size
		w.flush()
	}
	if len(pkg.Trace) > 0 {
		w.traces = append(w.traces, traceutil.APITrace(pkg.Trace))
	}
	w.events = append(w.events, pkg.Events...)
	w.bufferedSize += size
}

func (w *TraceWriter) resetBuffer() {
	w.bufferedSize = 0
	w.traces = w.traces[:0]
	w.events = w.events[:0]
}

const headerLanguages = "X-Datadog-Reported-Languages"

func (w *TraceWriter) flush() {
	if len(w.traces) == 0 && len(w.events) == 0 {
		// nothing to do
		return
	}

	defer timing.Since("datadog.trace_agent.trace_writer.encode_ms", time.Now())
	defer w.resetBuffer()

	payload := pb.TracePayload{
		HostName:     w.hostname,
		Env:          w.env,
		Traces:       w.traces,
		Transactions: w.events,
	}
	b, err := proto.Marshal(&payload)
	if err != nil {
		log.Errorf("Failed to serialize payload, data dropped: %s", err)
		return
	}
	var buf bytes.Buffer
	gzipw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		// it will never happen, unless an invalid compression is chosen;
		// we know gzip.BestSpeed is valid.
		log.Errorf("gzip.NewWriterLevel: %d", gzip.BestSpeed)
		return
	}
	gzipw.Write(b)
	gzipw.Close()

	req := newPayload(&buf, map[string]string{
		"Content-Type":     "application/x-protobuf",
		"Content-Encoding": "gzip",
		headerLanguages:    strings.Join(info.Languages(), "|"),
	})
	for _, sender := range w.senders {
		sender.send(req)
	}
}
