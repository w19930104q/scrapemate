package scrapemateapp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
)

func Test_getFetcher_WiresRotatorWhenProxiesConfigured(t *testing.T) {
	app := &ScrapemateApp{
		cfg: &Config{
			Proxies: []string{
				"http://proxy-0.example.com:8080",
				"http://proxy-1.example.com:8080",
			},
		},
	}

	fetcher, err := app.getFetcher()
	require.NoError(t, err)
	require.NotNil(t, app.rotator)
	require.Len(t, app.rotator.Proxies(), 2)

	// the fetcher must support the RefreshIP rebinding contract
	_, ok := fetcher.(scrapemate.ProxyRefreshableFetcher)
	require.True(t, ok)
}

func Test_getFetcher_HasNoRotatorWithoutProxies(t *testing.T) {
	app := &ScrapemateApp{
		cfg: &Config{},
	}

	fetcher, err := app.getFetcher()
	require.NoError(t, err)
	require.Nil(t, app.rotator)
	require.NotNil(t, fetcher)
}

func Test_getMate_AcceptsRotatorWiring(t *testing.T) {
	app := &ScrapemateApp{
		cfg: &Config{
			Concurrency:        1,
			MaxPagesPerBrowser: 1,
			Writers:            []scrapemate.ResultWriter{},
			Proxies: []string{
				"http://proxy-0.example.com:8080",
			},
		},
	}

	mate, err := app.getMate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, mate)
	require.NotNil(t, app.rotator)
}
