package writer

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSender(t *testing.T) {
	client := &http.Client{}

	t.Run("accept", func(t *testing.T) {
		assert := assert.New(t)
		server := newTestServer(t)
		defer server.Close()

		s := newSender(client, server.URL+"/", "123")
		for i := 0; i < 20; i++ {
			s.Push(payloadWithReaction(reactionAccept))
		}
		s.Flush()

		assert.Equal(20, server.Total(), "total")
		assert.Equal(20, server.Accepted(), "accepted")
		assert.Equal(0, server.Retried(), "retry")
		assert.Equal(0, server.Failed(), "failed")
	})

	t.Run("failed", func(t *testing.T) {
		assert := assert.New(t)
		server := newTestServer(t)
		defer server.Close()

		s := newSender(client, server.URL+"/", "123")
		for i := 0; i < 20; i++ {
			s.Push(payloadWithReaction(reactionFail))
		}
		s.Flush()

		assert.Equal(20, server.Total(), "total")
		assert.Equal(0, server.Accepted(), "accepted")
		assert.Equal(0, server.Retried(), "retry")
		assert.Equal(20, server.Failed(), "failed")
	})

	t.Run("retry,retry,accept", func(t *testing.T) {
		assert := assert.New(t)
		server := newTestServer(t)
		defer server.Close()
		defer useBackoffDuration(time.Millisecond)()

		s := newSender(client, server.URL+"/", "123")

		s.Push(payloadWithReaction(
			reactionRetry,
			reactionRetry,
			reactionAccept,
		))

		time.Sleep(5 * time.Millisecond)

		assert.Equal(3, server.Total(), "total")
		assert.Equal(2, server.Retried(), "retry")
		assert.Equal(1, server.Accepted(), "accepted")
	})
}
