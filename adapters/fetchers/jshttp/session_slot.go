package jshttp

import (
	"context"
	"errors"
	"sync"

	"github.com/mxschmitt/playwright-go"
)

var errNoPages = errors.New("no pages available")

type page interface {
	isClosed() bool
}

type slotRuntime interface {
	pageCount() int
	closeExtraPages() error
	closeBrowser() error
	primaryPage() (page, error)
	recreatePage() error
	recreateContext() error
	recreateBrowser() error
	recycleIfNeeded() error
}

type runtimeFactory interface {
	create(context.Context) (slotRuntime, error)
}

type sessionSlot struct {
	factory runtimeFactory
	runtime slotRuntime
	// stale marks a slot whose browser was launched with an outdated proxy.
	// It is set by RefreshProxy, possibly while the slot is busy; the next
	// acquirePage closes the old browser and lazily creates one through the
	// factory's currently pinned proxy.
	staleMu sync.Mutex
	stale   bool
}

func newSessionSlot(factory runtimeFactory) *sessionSlot {
	return &sessionSlot{factory: factory}
}

// markStale flags the slot so its next acquirePage rebuilds the browser. It
// never blocks on an in-flight request and does not interrupt the browser that
// is currently serving one.
func (s *sessionSlot) markStale() {
	s.staleMu.Lock()
	defer s.staleMu.Unlock()

	s.stale = true
}

func (s *sessionSlot) isStale() bool {
	s.staleMu.Lock()
	defer s.staleMu.Unlock()

	return s.stale
}

func (s *sessionSlot) clearStale() {
	s.staleMu.Lock()
	defer s.staleMu.Unlock()

	s.stale = false
}

func (s *sessionSlot) ensureReady(ctx context.Context) error {
	if s.runtime != nil {
		return nil
	}

	runtime, err := s.factory.create(ctx)
	if err != nil {
		return err
	}

	s.runtime = runtime

	return nil
}

func (s *sessionSlot) cleanup(_ context.Context) error {
	if s.runtime == nil {
		return nil
	}

	return s.runtime.closeExtraPages()
}

func (s *sessionSlot) close() error {
	if s.runtime == nil {
		return nil
	}

	return s.runtime.closeBrowser()
}

func (s *sessionSlot) acquirePage(ctx context.Context) (page, error) {
	if s.isStale() {
		if s.runtime != nil {
			if err := s.runtime.closeBrowser(); err != nil {
				return nil, err
			}
		}

		// drop the old browser so ensureReady creates one through the
		// factory's currently pinned proxy
		s.runtime = nil
		s.clearStale()
	}

	if err := s.ensureReady(ctx); err != nil {
		return nil, err
	}

	p, err := s.runtime.primaryPage()
	if err == nil && !p.isClosed() {
		return p, nil
	}

	if err := s.runtime.recreatePage(); err != nil {
		if err := s.runtime.recreateContext(); err != nil {
			return nil, s.runtime.recreateBrowser()
		}
	}

	return s.runtime.primaryPage()
}

func (s *sessionSlot) release(ctx context.Context) error {
	if err := s.cleanup(ctx); err != nil {
		return err
	}

	return s.runtime.recycleIfNeeded()
}

type playwrightRuntimeFactory struct {
	pw            *playwright.Playwright
	headless      bool
	disableImages bool
	proxyPool     *ProxyPool
	ua            string
	pinned        *AuthProxy
	mu            sync.Mutex
}

var _ pinnableFactory = (*playwrightRuntimeFactory)(nil)

// setPinned pins every browser created afterwards to authProxy. A nil value
// restores round robin selection from the proxy pool.
func (f *playwrightRuntimeFactory) setPinned(authProxy *AuthProxy) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pinned = authProxy
}

func (f *playwrightRuntimeFactory) selectedProxy() *AuthProxy {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pinned != nil {
		return f.pinned
	}

	if f.proxyPool != nil {
		return f.proxyPool.Next()
	}

	return nil
}

func (f *playwrightRuntimeFactory) create(context.Context) (slotRuntime, error) {
	authProxy := f.selectedProxy()

	b, err := newBrowser(f.pw, f.headless, f.disableImages, authProxy, f.ua)
	if err != nil {
		return nil, err
	}

	return &playwrightRuntime{
		browser:       b,
		pw:            f.pw,
		headless:      f.headless,
		disableImages: f.disableImages,
		proxyPool:     f.proxyPool,
		ua:            f.ua,
		pinned:        authProxy,
	}, nil
}

type playwrightRuntime struct {
	browser       *browser
	pw            *playwright.Playwright
	headless      bool
	disableImages bool
	proxyPool     *ProxyPool
	ua            string
	pinned        *AuthProxy
}

func (r *playwrightRuntime) selectedProxy() *AuthProxy {
	if r.pinned != nil {
		return r.pinned
	}

	if r.proxyPool != nil {
		return r.proxyPool.Next()
	}

	return nil
}

func (r *playwrightRuntime) pageCount() int {
	return len(r.browser.ctx.Pages())
}

func (r *playwrightRuntime) closeExtraPages() error {
	pages := r.browser.ctx.Pages()
	for i := 1; i < len(pages); i++ {
		pages[i].Close()
	}

	return nil
}

func (r *playwrightRuntime) closeBrowser() error {
	r.browser.Close()
	return nil
}

func (r *playwrightRuntime) primaryPage() (page, error) {
	pages := r.browser.ctx.Pages()
	if len(pages) == 0 {
		p, err := r.browser.ctx.NewPage()
		if err != nil {
			return nil, err
		}

		return &playwrightPage{p: p}, nil
	}

	return &playwrightPage{p: pages[0]}, nil
}

func (r *playwrightRuntime) recreatePage() error {
	pages := r.browser.ctx.Pages()
	for _, p := range pages {
		p.Close()
	}

	p, err := r.browser.ctx.NewPage()
	if err != nil {
		return err
	}

	r.browser.page0Usage = 0
	_ = p

	return nil
}

func (r *playwrightRuntime) recreateContext() error {
	r.browser.ctx.Close()

	ctx, err := r.browser.browser.NewContext()
	if err != nil {
		return err
	}

	r.browser.ctx = ctx
	r.browser.page0Usage = 0

	return nil
}

func (r *playwrightRuntime) recreateBrowser() error {
	r.browser.Close()

	b, err := newBrowser(r.pw, r.headless, r.disableImages, r.selectedProxy(), r.ua)
	if err != nil {
		return err
	}

	r.browser = b

	return nil
}

func (r *playwrightRuntime) recycleIfNeeded() error {
	return nil
}

type playwrightPage struct {
	p playwright.Page
}

func (p *playwrightPage) isClosed() bool {
	return !p.p.IsClosed()
}

func (p *playwrightPage) playwrightPage() playwright.Page {
	return p.p
}
