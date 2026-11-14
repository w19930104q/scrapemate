package scrapemate_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/mock"
)

// refreshableFetcher wraps the generated mock fetcher and records the proxies
// it is rebound to, so tests can assert that RefreshIP retries really use the
// proxy selected by the rotator.
type refreshableFetcher struct {
	*mock.MockHTTPFetcher

	mu        sync.Mutex
	refreshed []scrapemate.Proxy
}

func newRefreshableFetcher(ctrl *gomock.Controller) *refreshableFetcher {
	return &refreshableFetcher{
		MockHTTPFetcher: mock.NewMockHTTPFetcher(ctrl),
	}
}

func (f *refreshableFetcher) RefreshProxy(_ context.Context, proxy scrapemate.Proxy) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.refreshed = append(f.refreshed, proxy)

	return nil
}

func (f *refreshableFetcher) refreshedProxies() []scrapemate.Proxy {
	f.mu.Lock()
	defer f.mu.Unlock()

	ans := make([]scrapemate.Proxy, len(f.refreshed))
	copy(ans, f.refreshed)

	return ans
}

// refreshJob always fails the response check so doFetch keeps retrying until
// MaxRetries is reached.
type refreshJob struct {
	scrapemate.Job
}

func (j *refreshJob) DoCheckResponse(_ *scrapemate.Response) bool {
	return false
}

func rejectedResponse() scrapemate.Response {
	return scrapemate.Response{StatusCode: http.StatusBadGateway, Body: []byte("nope")}
}

func Test_WithProxyRotator_Nil(t *testing.T) {
	svc := getMockedServices(t)

	_, err := scrapemate.New(
		scrapemate.WithJobProvider(svc.provider),
		scrapemate.WithHTTPFetcher(svc.fetcher),
		scrapemate.WithProxyRotator(nil),
	)
	require.Error(t, err)
}

func Test_RefreshIP_RotatesProxyInOrderAndBacksOff(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	proxies := []string{
		"http://proxy-0.example.com:8080",
		"http://proxy-1.example.com:8080",
		"http://proxy-2.example.com:8080",
	}

	rotator.EXPECT().Proxies().Return(proxies).AnyTimes()

	gomock.InOrder(
		rotator.EXPECT().Next().Return(scrapemate.Proxy{URL: proxies[0]}),
		rotator.EXPECT().Next().Return(scrapemate.Proxy{URL: proxies[1]}),
		rotator.EXPECT().Next().Return(scrapemate.Proxy{URL: proxies[2]}),
	)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RefreshIP,
			MaxRetries:    3,
			MaxRetryDelay: 200 * time.Millisecond,
		},
	}

	// initial attempt + 3 retries
	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(4)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	start := time.Now()

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)

	elapsed := time.Since(start)

	// backoff sequence: 100ms, 200ms (capped), 200ms (capped)
	require.GreaterOrEqual(t, elapsed, 450*time.Millisecond)
	require.Less(t, elapsed, 900*time.Millisecond)

	got := fetcher.refreshedProxies()
	require.Len(t, got, 3)
	require.Equal(t, proxies[0], got[0].URL)
	require.Equal(t, proxies[1], got[1].URL)
	require.Equal(t, proxies[2], got[2].URL)
}

func Test_RefreshIP_RetryDelayIsCappedByMaxRetryDelay(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	rotator.EXPECT().Proxies().Return([]string{"http://proxy-0.example.com:8080"}).AnyTimes()
	rotator.EXPECT().Next().Return(scrapemate.Proxy{URL: "http://proxy-0.example.com:8080"}).Times(2)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RefreshIP,
			MaxRetries:    2,
			MaxRetryDelay: 50 * time.Millisecond,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(3)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	start := time.Now()

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)

	// both waits are capped to 50ms
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond)
	require.Less(t, elapsed, 300*time.Millisecond)
}

func Test_RefreshIP_RespectsContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	rotator.EXPECT().Proxies().Return([]string{"http://proxy-0.example.com:8080"}).AnyTimes()
	// cancellation must happen before the rotator is used
	rotator.EXPECT().Next().Times(0)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RefreshIP,
			MaxRetries:    3,
			MaxRetryDelay: 10 * time.Second,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(1)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancelCause(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel(errors.New("test cancellation"))
	}()

	start := time.Now()

	_, _, doErr := mate.DoJob(ctx, job)
	require.Error(t, doErr)

	require.Less(t, time.Since(start), time.Second)
	require.Empty(t, fetcher.refreshedProxies())
}

func Test_RefreshIP_WithoutRotator_DegradesToDelayedRetry(t *testing.T) {
	ctrl := gomock.NewController(t)

	fetcher := newRefreshableFetcher(ctrl)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RefreshIP,
			MaxRetries:    2,
			MaxRetryDelay: 50 * time.Millisecond,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(3)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
	)
	require.NoError(t, err)

	start := time.Now()

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)

	// the exponential backoff is still applied
	require.GreaterOrEqual(t, time.Since(start), 90*time.Millisecond)
	// no rebinding ever happens
	require.Empty(t, fetcher.refreshedProxies())
}

func Test_RefreshIP_WithEmptyRotator_DegradesToDelayedRetry(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	rotator.EXPECT().Proxies().Return(nil).AnyTimes()
	rotator.EXPECT().Next().Times(0)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RefreshIP,
			MaxRetries:    1,
			MaxRetryDelay: 50 * time.Millisecond,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(2)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)
	require.Empty(t, fetcher.refreshedProxies())
}

func Test_RetryJob_DoesNotTouchRotator(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	rotator.EXPECT().Proxies().AnyTimes()
	rotator.EXPECT().Next().Times(0)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:           "http://example.com",
			RetryPolicy:   scrapemate.RetryJob,
			MaxRetries:    2,
			MaxRetryDelay: 50 * time.Millisecond,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(3)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)
	require.Empty(t, fetcher.refreshedProxies())
}

func Test_DiscardJob_DoesNotTouchRotator(t *testing.T) {
	ctrl := gomock.NewController(t)

	rotator := mock.NewMockProxyRotator(ctrl)
	fetcher := newRefreshableFetcher(ctrl)

	rotator.EXPECT().Next().Times(0)

	job := &refreshJob{
		Job: scrapemate.Job{
			URL:         "http://example.com",
			RetryPolicy: scrapemate.DiscardJob,
			MaxRetries:  5,
		},
	}

	fetcher.EXPECT().Fetch(gomock.Any(), job).Return(rejectedResponse()).Times(1)
	fetcher.EXPECT().Close().AnyTimes()

	mate, err := scrapemate.New(
		scrapemate.WithJobProvider(mock.NewMockJobProvider(ctrl)),
		scrapemate.WithHTTPFetcher(fetcher),
		scrapemate.WithProxyRotator(rotator),
	)
	require.NoError(t, err)

	_, _, err = mate.DoJob(context.Background(), job)
	require.Error(t, err)
	require.Empty(t, fetcher.refreshedProxies())
}
