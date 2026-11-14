package jshttp

import (
	"fmt"
	"net/url"
	"sync"
)

type ProxyPool struct {
	current int
	mu      *sync.Mutex
	// urls holds the full proxy URLs (scrapemate.Proxy.FullURL() values) the
	// pool was created with, in the same order as proxies. It allows looking
	// up the local AuthProxy that corresponds to a proxy selected by
	// scrapemate's rotator.
	urls    []string
	proxies []*AuthProxy
}

func NewProxyPool(proxies []string) (*ProxyPool, error) {
	if len(proxies) == 0 {
		return nil, fmt.Errorf("no proxies provided")
	}

	pool := &ProxyPool{
		mu:      &sync.Mutex{},
		urls:    make([]string, 0, len(proxies)),
		proxies: make([]*AuthProxy, 0, len(proxies)),
	}

	for _, p := range proxies {
		cfg, err := parseProxy(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy %s: %w", p, err)
		}

		authProxy, err := StartAuthProxy(cfg.u, cfg.username, cfg.password)
		if err != nil {
			return nil, fmt.Errorf("failed to start auth proxy for %s: %w", p, err)
		}

		pool.urls = append(pool.urls, p)
		pool.proxies = append(pool.proxies, authProxy)
	}

	return pool, nil
}

func (pp *ProxyPool) Next() *AuthProxy {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	p := pp.proxies[pp.current%len(pp.proxies)]
	pp.current++

	return p
}

// Get returns the local AuthProxy that serves the upstream proxy identified by
// fullURL (a scrapemate.Proxy.FullURL() value). It is used when the
// RefreshIP retry policy asks the fetcher to bind the next request to the
// proxy selected by scrapemate's rotator.
func (pp *ProxyPool) Get(fullURL string) (*AuthProxy, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	for i, u := range pp.urls {
		if u == fullURL {
			return pp.proxies[i], true
		}
	}

	return nil, false
}

type proxyConfig struct {
	u        string
	username string
	password string
}

func parseProxy(proxy string) (*proxyConfig, error) {
	if proxy == "" {
		return nil, fmt.Errorf("proxy URL cannot be empty")
	}

	u, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	u.User = nil // Clear user info from URL

	return &proxyConfig{
		u:        u.String(),
		username: username,
		password: password,
	}, nil
}
