package nethttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/gosom/scrapemate"
)

var _ scrapemate.HTTPFetcher = (*httpFetch)(nil)

var _ scrapemate.ProxyRefreshableFetcher = (*httpFetch)(nil)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func New(netClient HTTPClient) scrapemate.HTTPFetcher {
	return &httpFetch{
		netClient: netClient,
	}
}

// NewWithRotator creates an HTTP fetcher whose requests are sent through the
// given rotator. By default every request is sent via the next proxy of the
// rotator (round robin). Jobs using the RefreshIP policy pin the next request
// to the proxy selected by scrapemate's rotator via RefreshProxy; once that
// request has been served, round robin resumes.
//
// The rotator is installed as the transport of netClient, so netClient should
// not carry its own Transport when passed here.
func NewWithRotator(netClient *http.Client, rotator scrapemate.ProxyRotator) scrapemate.HTTPFetcher {
	client := *netClient
	client.Transport = newRotatingTransport(rotator)

	return &httpFetch{
		netClient: &client,
		rotator:   rotator,
	}
}

type httpFetch struct {
	netClient HTTPClient
	rotator   scrapemate.ProxyRotator
	// pinnedProxy, when set, is used for the next Fetch call only. It is how
	// the RefreshIP retry policy makes a retry use the proxy chosen by
	// scrapemate's rotator.
	pinnedMu    sync.Mutex
	pinnedProxy *scrapemate.Proxy
}

// RefreshProxy pins the next Fetch call to proxy. It implements
// scrapemate.ProxyRefreshableFetcher. The pin is consumed by the next request;
// afterwards the fetcher goes back to round robin rotation.
func (o *httpFetch) RefreshProxy(_ context.Context, proxy scrapemate.Proxy) error {
	o.pinnedMu.Lock()
	defer o.pinnedMu.Unlock()

	pinned := proxy
	o.pinnedProxy = &pinned

	return nil
}

func (o *httpFetch) Close() error {
	return nil
}

func (o *httpFetch) Fetch(ctx context.Context, job scrapemate.IJob) scrapemate.Response {
	u := job.GetFullURL()
	reqBody := getBuffer()

	defer putBuffer(reqBody)

	if len(job.GetBody()) > 0 {
		reqBody.Write(job.GetBody())
	}

	var ans scrapemate.Response

	req, err := http.NewRequestWithContext(ctx, job.GetMethod(), u, reqBody)
	if err != nil {
		ans.Error = err
		return ans
	}

	if o.rotator != nil {
		o.pinnedMu.Lock()
		pinned := o.pinnedProxy
		o.pinnedProxy = nil
		o.pinnedMu.Unlock()

		if pinned != nil {
			ctx = context.WithValue(req.Context(), pinnedProxyContextKey{}, pinned)
			req = req.WithContext(ctx)
		}
	}

	for k, v := range job.GetHeaders() {
		req.Header.Add(k, v)
	}

	resp, err := o.netClient.Do(req)
	if err != nil {
		ans.Error = err
		return ans
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	ans.StatusCode = resp.StatusCode
	ans.Headers = http.Header{}

	for k, v := range resp.Header {
		ans.Headers[k] = v
	}

	var reader io.ReadCloser

	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			ans.Error = err
			return ans
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	ans.Body, ans.Error = io.ReadAll(reader)
	ans.URL = resp.Request.URL.String()

	return ans
}

type pinnedProxyContextKey struct{}

// rotatingTransport sends every request through the wrapped rotator, except
// when the request context carries a pinned proxy (set via RefreshProxy), in
// which case that exact proxy is used for the request.
type rotatingTransport struct {
	rotator    scrapemate.ProxyRotator
	transports sync.Map // proxy URL -> *http.Transport
}

func newRotatingTransport(rotator scrapemate.ProxyRotator) *rotatingTransport {
	return &rotatingTransport{rotator: rotator}
}

func (t *rotatingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	proxy := t.proxyForRequest(req)

	transport, err := t.transportFor(proxy)
	if err != nil {
		return nil, err
	}

	return transport.RoundTrip(req)
}

func (t *rotatingTransport) proxyForRequest(req *http.Request) scrapemate.Proxy {
	if pinned, ok := req.Context().Value(pinnedProxyContextKey{}).(*scrapemate.Proxy); ok && pinned != nil {
		return *pinned
	}

	return t.rotator.Next()
}

func (t *rotatingTransport) transportFor(proxy scrapemate.Proxy) (*http.Transport, error) {
	if cached, ok := t.transports.Load(proxy.URL); ok {
		transport, ok := cached.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("unexpected cached transport type for proxy %s", proxy.URL)
		}

		return transport, nil
	}

	proxyURL, err := url.Parse(proxy.FullURL())
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}

	actual, _ := t.transports.LoadOrStore(proxy.URL, transport)

	stored, ok := actual.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("unexpected cached transport type for proxy %s", proxy.URL)
	}

	return stored, nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	//nolint:errcheck // we don't care about errors here
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()

	return b
}

func putBuffer(buf *bytes.Buffer) {
	bufferPool.Put(buf)
}
