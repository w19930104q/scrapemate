package jshttp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mxschmitt/playwright-go"

	"github.com/gosom/scrapemate"
	playwrightadapter "github.com/gosom/scrapemate/adapters/browsers/playwright"
)

var _ scrapemate.HTTPFetcher = (*jsFetch)(nil)

// closeTimeout is the maximum time allowed for a Playwright page.Close() before
// the caller abandons it. A wedged Playwright driver (e.g. EPIPE after a browser
// crash) can make Close block forever; abandoning it frees the worker goroutine
// at the cost of leaking the underlying resource.
const closeTimeout = 5 * time.Second

// closeWithTimeout runs closer in a background goroutine and returns when it
// completes or when d elapses, whichever comes first. On timeout the goroutine
// is abandoned (it will finish on its own if the driver recovers). Without this
// bound, a hung Close propagates through the worker WaitGroup and prevents the
// caller from being unblocked even by context cancellation.
func closeWithTimeout(closer func() error, d time.Duration) {
	done := make(chan struct{})

	go func() {
		_ = closer()

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(d):
	}
}

type JSFetcherOptions struct {
	Headless           bool
	DisableImages      bool
	Rotator            scrapemate.ProxyRotator
	PoolSize           int
	MaxPagesPerBrowser int
	PageReuseLimit     int
	BrowserReuseLimit  int
	UserAgent          string
}

func New(params JSFetcherOptions) (scrapemate.HTTPFetcher, error) {
	opts := []*playwright.RunOptions{
		{
			Browsers: []string{"chromium"},
			Verbose:  true,
		},
	}

	if err := playwright.Install(opts...); err != nil {
		return nil, err
	}

	pw, err := playwright.Run()
	if err != nil {
		return nil, err
	}

	var pool *ProxyPool

	if params.Rotator != nil {
		proxies := params.Rotator.Proxies()

		if len(proxies) > 0 {
			pool, err = NewProxyPool(proxies)
			if err != nil {
				return nil, err
			}
		}
	}

	maxPagesPerBrowser := params.MaxPagesPerBrowser
	if maxPagesPerBrowser < 1 {
		maxPagesPerBrowser = 1
	}

	ans := jsFetch{
		pw:                 pw,
		headless:           params.Headless,
		disableImages:      params.DisableImages,
		pageReuseLimit:     params.PageReuseLimit,
		browserReuseLimit:  params.BrowserReuseLimit,
		ua:                 params.UserAgent,
		proxyPool:          pool,
		rotator:            params.Rotator,
		maxPagesPerBrowser: maxPagesPerBrowser,
	}

	if maxPagesPerBrowser > 1 {
		slotFactory := &playwrightSlotFactory{
			pw:            pw,
			headless:      params.Headless,
			disableImages: params.DisableImages,
			proxyPool:     pool,
			ua:            params.UserAgent,
		}

		ans.pageSlots, err = newPageSlotPool(pageSlotPoolConfig{
			poolSize:           params.PoolSize,
			maxPagesPerBrowser: maxPagesPerBrowser,
			factory:            slotFactory,
		})
		if err != nil {
			_ = ans.Close()

			return nil, err
		}

		ans.pageSlotFactory = slotFactory
		ans.poolSize = params.PoolSize

		return &ans, nil
	}

	ans.slots = make(chan *sessionSlot, params.PoolSize)

	sessionFactory := &playwrightRuntimeFactory{
		pw:            pw,
		headless:      params.Headless,
		disableImages: params.DisableImages,
		proxyPool:     pool,
		ua:            params.UserAgent,
	}
	ans.factory = sessionFactory
	ans.poolSize = params.PoolSize
	ans.allSlots = make([]*sessionSlot, 0, params.PoolSize)

	for range params.PoolSize {
		slot := newSessionSlot(sessionFactory)
		ans.allSlots = append(ans.allSlots, slot)

		ans.slots <- slot
	}

	return &ans, nil
}

type jsFetch struct {
	pw                *playwright.Playwright
	headless          bool
	disableImages     bool
	pageReuseLimit    int
	browserReuseLimit int
	ua                string
	proxyPool         *ProxyPool
	factory           runtimeFactory
	slots             chan *sessionSlot
	// allSlots holds every session slot, including the ones currently
	// acquired by in-flight requests, so RefreshProxy can mark them all stale.
	allSlots        []*sessionSlot
	pageSlots       *pageSlotPool
	pageSlotFactory pageSlotFactory
	poolSize        int

	rotator            scrapemate.ProxyRotator
	maxPagesPerBrowser int

	rebindMu sync.Mutex
}

func (o *jsFetch) getSlot(ctx context.Context) (*sessionSlot, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case slot := <-o.slots:
		return slot, nil
	}
}

func (o *jsFetch) putSlot(ctx context.Context, slot *sessionSlot) {
	select {
	case <-ctx.Done():
		_ = slot.close()
	case o.slots <- slot:
	}
}

func (o *jsFetch) Close() error {
	if o.pageSlots != nil {
		o.pageSlots.close()
	}

	if o.slots != nil {
		close(o.slots)

		for slot := range o.slots {
			slot.close()
		}
	}

	_ = o.pw.Stop()

	return nil
}

// pinnableFactory is implemented by browser factories that can pin every
// browser they create afterwards to a specific local proxy. It is the seam
// used by RefreshProxy to rebuild sessions bound to the proxy selected by
// scrapemate's rotator.
type pinnableFactory interface {
	setPinned(*AuthProxy)
}

// RefreshProxy rebinds the fetcher to the local AuthProxy that serves proxy so
// that the next Fetch really runs through a different upstream IP.
//
// Playwright binds the proxy at browser launch time, so running browsers
// cannot be switched in place. The browser factory is pinned to the selected
// proxy and the existing sessions are marked stale (or, in the multi-page
// pool mode, the pool is rebuilt); the next request then runs on a browser
// launched through the new proxy. It implements
// scrapemate.ProxyRefreshableFetcher.
func (o *jsFetch) RefreshProxy(ctx context.Context, proxy scrapemate.Proxy) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if o.proxyPool == nil {
		return errors.New("cannot refresh proxy: no proxy pool is configured")
	}

	authProxy, ok := o.proxyPool.Get(proxy.FullURL())
	if !ok {
		return fmt.Errorf("proxy %s is not part of the fetcher's proxy pool", proxy.FullURL())
	}

	o.rebindMu.Lock()
	defer o.rebindMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if o.maxPagesPerBrowser > 1 {
		return o.rebindPageSlotPool(ctx, authProxy)
	}

	return o.rebindSessionSlots(ctx, authProxy)
}

// rebindSessionSlots pins the runtime factory to authProxy and marks every
// slot stale. Idle slots rebuild their browser on their next acquirePage;
// slots held by in-flight requests keep serving the current request and
// rebuild on the next one, so this never blocks on a busy worker.
func (o *jsFetch) rebindSessionSlots(_ context.Context, authProxy *AuthProxy) error {
	if f, ok := o.factory.(pinnableFactory); ok {
		f.setPinned(authProxy)
	}

	for _, slot := range o.allSlots {
		slot.markStale()
	}

	return nil
}

// rebindPageSlotPool closes the current browser pool (each browser keeps the
// proxy it was launched with) and builds a new one whose factory is pinned to
// authProxy.
func (o *jsFetch) rebindPageSlotPool(ctx context.Context, authProxy *AuthProxy) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f, ok := o.pageSlotFactory.(pinnableFactory)
	if !ok {
		return errors.New("page slot factory does not support proxy pinning")
	}

	f.setPinned(authProxy)

	pool, err := newPageSlotPool(pageSlotPoolConfig{
		poolSize:           o.poolSize,
		maxPagesPerBrowser: o.maxPagesPerBrowser,
		factory:            o.pageSlotFactory,
	})
	if err != nil {
		return err
	}

	old := o.pageSlots
	o.pageSlots = pool

	old.close()

	return nil
}

// Fetch fetches the url specicied by the job and returns the response
func (o *jsFetch) Fetch(ctx context.Context, job scrapemate.IJob) scrapemate.Response {
	if o.maxPagesPerBrowser > 1 {
		return o.fetchWithPageSlot(ctx, job)
	}

	slot, err := o.getSlot(ctx)
	if err != nil {
		return scrapemate.Response{
			Error: err,
		}
	}

	defer o.putSlot(ctx, slot)

	p, err := slot.acquirePage(ctx)
	if err != nil {
		return scrapemate.Response{
			Error: err,
		}
	}

	pp, ok := p.(*playwrightPage)
	if !ok {
		return scrapemate.Response{
			Error: errors.New("unexpected page type"),
		}
	}

	if job.GetTimeout() > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, job.GetTimeout())

		defer cancel()

		pp.playwrightPage().SetDefaultTimeout(float64(job.GetTimeout().Milliseconds()))
	}

	resp := runBrowserActions(ctx, job, playwrightadapter.NewPage(pp.playwrightPage()))

	if cleanErr := slot.release(ctx); cleanErr != nil && resp.Error == nil {
		resp.Error = cleanErr
	}

	return resp
}

func (o *jsFetch) fetchWithPageSlot(ctx context.Context, job scrapemate.IJob) scrapemate.Response {
	if job.GetTimeout() > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, job.GetTimeout())

		defer cancel()
	}

	lease, err := o.pageSlots.acquire(ctx)
	if err != nil {
		return scrapemate.Response{Error: err}
	}

	defer lease.release(ctx)

	page, err := lease.slot.ctx.NewPage()
	if err != nil {
		return scrapemate.Response{Error: err}
	}

	defer closeWithTimeout(func() error { return page.Close() }, closeTimeout)

	if job.GetTimeout() > 0 {
		page.SetDefaultTimeout(float64(job.GetTimeout().Milliseconds()))
	}

	lease.slot.mu.Lock()
	lease.slot.browserUsage++
	lease.slot.mu.Unlock()

	return runBrowserActions(ctx, job, playwrightadapter.NewPage(page))
}

func runBrowserActions(ctx context.Context, job scrapemate.IJob, page scrapemate.BrowserPage) scrapemate.Response {
	if hooks, ok := page.(interface{ ClearNetworkHooks() }); ok {
		defer hooks.ClearNetworkHooks()
	}

	return job.BrowserActions(ctx, page)
}

type browser struct {
	browser      playwright.Browser
	ctx          playwright.BrowserContext
	page0Usage   int
	browserUsage int
}

func (o *browser) Close() {
	_ = o.ctx.Close()
	_ = o.browser.Close()
}

func newBrowser(pw *playwright.Playwright, headless, disableImages bool, authProxy *AuthProxy, ua string) (*browser, error) {
	opts := playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(headless),
		Args: []string{
			`--start-maximized`,
			`--no-default-browser-check`,
			`--disable-dev-shm-usage`,
			`--no-sandbox`,
			`--disable-setuid-sandbox`,
			`--no-zygote`,
			`--disable-gpu`,
			`--mute-audio`,
			`--disable-extensions`,
			`--single-process`,
			`--disable-breakpad`,
			`--disable-features=TranslateUI,BlinkGenPropertyTrees`,
			`--disable-ipc-flooding-protection`,
			`--enable-features=NetworkService,NetworkServiceInProcess`,
			"--enable-features=NetworkService",
			`--disable-default-apps`,
			`--disable-notifications`,
			`--disable-webgl`,
			`--disable-blink-features=AutomationControlled`,
			"--ignore-certificate-errors",
			"--ignore-certificate-errors-spki-list",
			"--disable-web-security",
		},
	}
	if disableImages {
		opts.Args = append(opts.Args, `--blink-settings=imagesEnabled=false`)
	}

	br, err := pw.Chromium.Launch(opts)
	if err != nil {
		return nil, err
	}

	const defaultWidth, defaultHeight = 1920, 1080

	bctx, err := br.NewContext(playwright.BrowserNewContextOptions{
		UserAgent: func() *string {
			if ua == "" {
				defaultUA := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

				return &defaultUA
			}

			return &ua
		}(),
		Viewport: &playwright.Size{
			Width:  defaultWidth,
			Height: defaultHeight,
		},
		Proxy: func() *playwright.Proxy {
			if authProxy != nil {
				return &playwright.Proxy{
					Server: authProxy.Address(),
				}
			}

			return nil
		}(),
	})
	if err != nil {
		return nil, err
	}

	ans := browser{
		browser: br,
		ctx:     bctx,
	}

	return &ans, nil
}
