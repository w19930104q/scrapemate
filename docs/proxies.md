## Proxy Support

Scrapemate has builtin proxy support. 
You can add your proxies and each request will be fetched via the proxy.

When more that one proxy is provided they will be used in a round robin way.

To use proxies add the option `scrapemateapp.WithProxies()` to the config constructor.

example:

```go
proxies := []string{
	 "socks5://localhost:9050",
	 "http://user:pass@localhost:9051,
}

cfg, err := scrapemateapp.NewConfig(
	writers,
	scrapemateapp.WithConcurrency(1),
	scrapemateapp.WithProxies(proxies),
	scrapemateapp.WithExitOnInactivity(time.Minute)
)
```

## Refreshing the IP on retry

A job can ask scrapemate to switch to a different proxy before every retry by
setting its retry policy to `scrapemate.RefreshIP`:

```go
job := scrapemate.Job{
	URL:           "https://example.com",
	RetryPolicy:   scrapemate.RefreshIP,
	MaxRetries:    3,
	MaxRetryDelay: 2 * time.Second,
}
```

When such a job is rejected, scrapemate:

1. waits with the same exponential backoff as a normal retry, capped by the
   job's `MaxRetryDelay`;
2. advances the proxy rotator to the next available proxy;
3. rebinds the fetcher so that the retry really runs through the new proxy
   (the js fetcher rebuilds the browser session bound to the new proxy, the
   http and stealth fetchers pin their next request to it);
4. honours context cancellation for both the wait and the rebind.

The same mechanism is available when using scrapemate directly via
`scrapemate.WithProxyRotator`. If no rotator is configured (or it has no
proxies), `RefreshIP` degrades gracefully to a plain delayed retry and logs a
warning; `RetryJob` and `DiscardJob` are unaffected.
