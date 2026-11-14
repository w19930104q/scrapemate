package nethttp_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/fetchers/nethttp"
)

// fakeRotator is a hand-written rotator (the generated mock lives in the
// scrapemate/mock package and would create an import cycle risk in adapter
// tests). It records which proxies were handed out.
type fakeRotator struct {
	mu       sync.Mutex
	proxies  []scrapemate.Proxy
	cursor   int
	nextSeen []string
}

func newFakeRotator(proxies ...scrapemate.Proxy) *fakeRotator {
	return &fakeRotator{proxies: proxies}
}

func (r *fakeRotator) Next() scrapemate.Proxy {
	r.mu.Lock()
	defer r.mu.Unlock()

	p := r.proxies[r.cursor%len(r.proxies)]
	r.cursor++
	r.nextSeen = append(r.nextSeen, p.URL)

	return p
}

func (r *fakeRotator) Proxies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	ans := make([]string, len(r.proxies))
	for i, p := range r.proxies {
		ans[i] = p.FullURL()
	}

	return ans
}

func (r *fakeRotator) RoundTrip(req *http.Request) (*http.Response, error) {
	return http.DefaultTransport.RoundTrip(req)
}

func (r *fakeRotator) nextCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.nextSeen)
}

// newIdentifyingProxy starts an HTTP proxy that appends an identifying header
// to every forwarded request, so tests can tell which proxy served a request.
func newIdentifyingProxy(t *testing.T, id string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		//nolint:gosec // G704: test-only proxy forwarding to a local httptest server.
		outReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), req.Body)
		require.NoError(t, err)

		outReq.Header = req.Header.Clone()
		outReq.Header.Set("X-Via-Proxy", id)

		resp, err := http.DefaultTransport.RoundTrip(outReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}

		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
}

func newTargetServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		//nolint:gosec // G705: the header is a fixed test identifier, not user input.
		_, _ = w.Write([]byte(req.Header.Get("X-Via-Proxy")))
	}))
}

func proxyFromServer(t *testing.T, server *httptest.Server) scrapemate.Proxy {
	t.Helper()

	p, err := scrapemate.NewProxy(server.URL)
	require.NoError(t, err)

	return p
}

func Test_Fetch_RoundRobinRotatesProxies(t *testing.T) {
	proxy1 := newIdentifyingProxy(t, "p1")
	proxy2 := newIdentifyingProxy(t, "p2")
	target := newTargetServer(t)

	rotator := newFakeRotator(proxyFromServer(t, proxy1), proxyFromServer(t, proxy2))

	fetcher := nethttp.NewWithRotator(http.DefaultClient, rotator)
	defer func() {
		require.NoError(t, fetcher.Close())
	}()

	job := &scrapemate.Job{URL: target.URL, Method: http.MethodGet}

	bodies := make([]string, 0, 3)

	for range 3 {
		resp := fetcher.Fetch(context.Background(), job)
		require.Nil(t, resp.Error)
		bodies = append(bodies, string(resp.Body))
	}

	require.Equal(t, []string{"p1", "p2", "p1"}, bodies)
	require.Equal(t, 3, rotator.nextCount())
}

func Test_RefreshProxy_PinsNextRequestToSelectedProxy(t *testing.T) {
	proxy1 := newIdentifyingProxy(t, "p1")
	proxy2 := newIdentifyingProxy(t, "p2")
	target := newTargetServer(t)

	p1 := proxyFromServer(t, proxy1)
	p2 := proxyFromServer(t, proxy2)

	rotator := newFakeRotator(p1, p2)

	fetcher := nethttp.NewWithRotator(http.DefaultClient, rotator)
	defer func() {
		require.NoError(t, fetcher.Close())
	}()

	job := &scrapemate.Job{URL: target.URL, Method: http.MethodGet}

	// first request goes through round robin -> p1
	resp := fetcher.Fetch(context.Background(), job)
	require.Nil(t, resp.Error)
	require.Equal(t, "p1", string(resp.Body))

	// simulate scrapemate's RefreshIP policy: pin the next request to p2
	refreshable, ok := fetcher.(scrapemate.ProxyRefreshableFetcher)
	require.True(t, ok)
	require.NoError(t, refreshable.RefreshProxy(context.Background(), p2))

	resp = fetcher.Fetch(context.Background(), job)
	require.Nil(t, resp.Error)
	require.Equal(t, "p2", string(resp.Body))

	// the pin is one-shot: round robin resumes and selects p2 again
	resp = fetcher.Fetch(context.Background(), job)
	require.Nil(t, resp.Error)
	require.Equal(t, "p2", string(resp.Body))

	// only 2 round-robin calls: the pinned request did not advance the rotator
	require.Equal(t, 2, rotator.nextCount())
}

func Test_RefreshProxy_CancelledContextIsStillConsumedByNextFetch(t *testing.T) {
	proxy1 := newIdentifyingProxy(t, "p1")
	target := newTargetServer(t)

	p1 := proxyFromServer(t, proxy1)
	rotator := newFakeRotator(p1)

	fetcher := nethttp.NewWithRotator(http.DefaultClient, rotator)
	defer func() {
		require.NoError(t, fetcher.Close())
	}()

	refreshable, ok := fetcher.(scrapemate.ProxyRefreshableFetcher)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// nethttp's rebind never blocks, so even a cancelled context accepts the pin
	require.NoError(t, refreshable.RefreshProxy(ctx, p1))

	job := &scrapemate.Job{URL: target.URL, Method: http.MethodGet}
	resp := fetcher.Fetch(context.Background(), job)
	require.Nil(t, resp.Error)
	require.Equal(t, "p1", string(resp.Body))
}
