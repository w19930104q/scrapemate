package scrapemate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gosom/kit/logging"
	"github.com/stretchr/testify/require"
)

func Test_nextBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		current  time.Duration
		maxDelay time.Duration
		expected time.Duration
	}{
		{"doubles below cap", 100 * time.Millisecond, time.Second, 200 * time.Millisecond},
		{"caps when doubling exceeds max", 600 * time.Millisecond, time.Second, time.Second},
		{"stays at cap", time.Second, time.Second, time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.expected, nextBackoff(tt.current, tt.maxDelay))
		})
	}
}

func Test_waitRetryDelay(t *testing.T) {
	t.Parallel()

	t.Run("waits the full delay", func(t *testing.T) {
		t.Parallel()

		s := &ScrapeMate{log: logging.Get()}

		start := time.Now()
		ok := s.waitRetryDelay(context.Background(), &Job{}, 50*time.Millisecond)

		require.True(t, ok)
		require.GreaterOrEqual(t, time.Since(start), 45*time.Millisecond)
	})

	t.Run("returns false on context cancellation", func(t *testing.T) {
		t.Parallel()

		s := &ScrapeMate{log: logging.Get()}

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errors.New("test"))

		start := time.Now()
		ok := s.waitRetryDelay(ctx, &Job{}, time.Minute)

		require.False(t, ok)
		require.Less(t, time.Since(start), 100*time.Millisecond)
	})
}
