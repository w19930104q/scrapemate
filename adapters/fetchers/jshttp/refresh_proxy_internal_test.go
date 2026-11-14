package jshttp

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
)

func newTestMu() *sync.Mutex {
	return &sync.Mutex{}
}

// newLocalAuthProxy starts a real local AuthProxy backed by an inert upstream
// listener. We only need its Address()/identity for pinning tests; no request
// is ever sent through it.
func newLocalAuthProxy(t *testing.T) *AuthProxy {
	t.Helper()

	upstream := httptest.NewServer(nil)
	t.Cleanup(upstream.Close)

	authProxy, err := StartAuthProxy(upstream.URL, "user", "pass")
	require.NoError(t, err)
	t.Cleanup(func() { _ = authProxy.Close() })

	return authProxy
}

func newTestProxyPool(t *testing.T, proxies ...*AuthProxy) *ProxyPool {
	t.Helper()

	urls := make([]string, len(proxies))
	for i, p := range proxies {
		urls[i] = "http://" + p.Address()
	}

	// bypass StartAuthProxy by constructing the pool directly: tests only need
	// Get/Next bookkeeping.
	pool := &ProxyPool{
		mu:      newTestMu(),
		urls:    urls,
		proxies: proxies,
	}

	return pool
}

func TestProxyPoolGetReturnsAuthProxyByFullURL(t *testing.T) {
	t.Parallel()

	ap1 := newLocalAuthProxy(t)
	ap2 := newLocalAuthProxy(t)

	pool := newTestProxyPool(t, ap1, ap2)

	got, ok := pool.Get("http://" + ap2.Address())
	require.True(t, ok)
	require.Same(t, ap2, got)

	_, ok = pool.Get("http://unknown:9999")
	require.False(t, ok)
}

func TestRefreshProxySessionSlotsRebuildsSessionsPinnedToProxy(t *testing.T) {
	factory := &fakeRuntimeFactory{}
	slots := make(chan *sessionSlot, 2)

	allSlots := make([]*sessionSlot, 0, 2)

	for range 2 {
		slot := newSessionSlot(factory)
		allSlots = append(allSlots, slot)

		slots <- slot
	}

	ap1 := newLocalAuthProxy(t)
	ap2 := newLocalAuthProxy(t)
	pool := newTestProxyPool(t, ap1, ap2)

	fetcher := &jsFetch{
		slots:     slots,
		allSlots:  allSlots,
		factory:   factory,
		proxyPool: pool,
	}

	// create runtimes so there are live browsers to rebuild
	for range 2 {
		slot := <-slots
		require.NoError(t, slot.ensureReady(context.Background()))

		slots <- slot
	}

	initialCreations := factory.browserCreations
	require.Equal(t, 2, initialCreations)

	target := scrapemate.Proxy{URL: "http://" + ap2.Address()}

	require.NoError(t, fetcher.RefreshProxy(context.Background(), target))

	// the factory is pinned and every slot is flagged stale, but no browser is
	// rebuilt eagerly
	require.Same(t, ap2, factory.pinned)
	require.True(t, allSlots[0].isStale())
	require.True(t, allSlots[1].isStale())
	require.Equal(t, initialCreations, factory.browserCreations)
	require.Zero(t, factory.closeCount)

	// all slots are still in the pool
	require.Len(t, slots, 2)

	// the first acquire lazily closes the old browser and creates a pinned one
	slot := <-slots

	acquired, err := slot.acquirePage(context.Background())
	require.NoError(t, err)
	require.NotNil(t, acquired)
	require.False(t, slot.isStale())
	require.Equal(t, 1, factory.closeCount)
	require.Equal(t, initialCreations+1, factory.browserCreations)
}

func TestRefreshProxySessionSlotsMarksBusySlotsStaleWithoutBlocking(t *testing.T) {
	factory := &fakeRuntimeFactory{}
	slots := make(chan *sessionSlot, 2)

	idle := newSessionSlot(factory)
	busy := newSessionSlot(factory)
	allSlots := []*sessionSlot{idle, busy}

	slots <- idle
	// busy stays outside the channel, as if a worker were holding it

	ap1 := newLocalAuthProxy(t)
	pool := newTestProxyPool(t, ap1)

	fetcher := &jsFetch{
		slots:     slots,
		allSlots:  allSlots,
		factory:   factory,
		proxyPool: pool,
	}

	done := make(chan error, 1)

	go func() {
		done <- fetcher.RefreshProxy(context.Background(), scrapemate.Proxy{
			URL: "http://" + ap1.Address(),
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		require.Fail(t, "RefreshProxy blocked while a slot was busy")
	}

	// both the idle and the busy slot are marked stale, including the one a
	// worker currently holds
	require.Same(t, ap1, factory.pinned)
	require.True(t, idle.isStale())
	require.True(t, busy.isStale())
	require.Len(t, slots, 1)

	// when the busy slot is returned, its next acquire rebuilds through the
	// pinned proxy without disturbing the pool
	slots <- busy

	require.Len(t, slots, 2)

	slot := <-slots // idle slot first
	_, err := slot.acquirePage(context.Background())
	require.NoError(t, err)
}

func TestRefreshProxyWithoutPoolReturnsError(t *testing.T) {
	fetcher := &jsFetch{}

	err := fetcher.RefreshProxy(context.Background(), scrapemate.Proxy{URL: "http://example:8080"})
	require.Error(t, err)
}

func TestRefreshProxyWithUnknownProxyReturnsError(t *testing.T) {
	ap1 := newLocalAuthProxy(t)
	pool := newTestProxyPool(t, ap1)

	fetcher := &jsFetch{proxyPool: pool}

	err := fetcher.RefreshProxy(context.Background(), scrapemate.Proxy{URL: "http://unknown:9999"})
	require.Error(t, err)
}

func TestRefreshProxySessionSlotsRespectsCancellation(t *testing.T) {
	factory := &fakeRuntimeFactory{}
	slots := make(chan *sessionSlot, 1)

	slots <- newSessionSlot(factory)

	ap1 := newLocalAuthProxy(t)
	pool := newTestProxyPool(t, ap1)

	fetcher := &jsFetch{
		slots:     slots,
		factory:   factory,
		proxyPool: pool,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fetcher.RefreshProxy(ctx, scrapemate.Proxy{URL: "http://" + ap1.Address()})
	require.ErrorIs(t, err, context.Canceled)

	// slot must still be available for normal fetching afterwards and the
	// factory was never pinned
	require.Len(t, slots, 1)
	require.Nil(t, factory.pinned)
}

func TestRefreshProxyPageSlotPoolRebuildsPoolPinnedToProxy(t *testing.T) {
	factory := &fakeSlotFactory{}

	initial, err := newPageSlotPool(pageSlotPoolConfig{
		poolSize:           2,
		maxPagesPerBrowser: 2,
		factory:            factory,
	})
	require.NoError(t, err)

	ap1 := newLocalAuthProxy(t)
	ap2 := newLocalAuthProxy(t)
	proxyPool := newTestProxyPool(t, ap1, ap2)

	fetcher := &jsFetch{
		pageSlots:          initial,
		pageSlotFactory:    factory,
		proxyPool:          proxyPool,
		poolSize:           2,
		maxPagesPerBrowser: 2,
	}

	require.Equal(t, int64(2), factory.created.Load())

	require.NoError(t, fetcher.RefreshProxy(context.Background(), scrapemate.Proxy{
		URL: "http://" + ap2.Address(),
	}))

	// factory got pinned and a fresh pool was built with new slots
	require.Same(t, ap2, factory.pinned)
	require.NotSame(t, initial, fetcher.pageSlots)
	require.Equal(t, int64(4), factory.created.Load())

	// the new pool serves leases
	lease, err := fetcher.pageSlots.acquire(context.Background())
	require.NoError(t, err)
	lease.release(context.Background())
}
