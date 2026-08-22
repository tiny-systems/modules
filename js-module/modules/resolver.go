package modules

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grafana/sobek"
	"github.com/grafana/sobek/parser"
)

// Resolver resolves ES module specifiers from a virtual filesystem or HTTP URLs.
type Resolver struct {
	mu    sync.Mutex
	cache map[string]cacheEntry
	fs    fs.FS
}

type cacheEntry struct {
	mod sobek.ModuleRecord
	err error
}

// NewResolver creates a resolver backed by the given filesystem.
// Local specifiers are read from fs, URLs starting with http:// or https:// are fetched.
func NewResolver(fsys fs.FS) *Resolver {
	return &Resolver{
		fs:    fsys,
		cache: make(map[string]cacheEntry),
	}
}

// Resolve implements the sobek module resolve callback.
func (r *Resolver) Resolve(_ interface{}, specifier string) (sobek.ModuleRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.cache[specifier]; ok {
		return entry.mod, entry.err
	}

	var (
		src []byte
		err error
	)

	if strings.HasPrefix(specifier, "http://") || strings.HasPrefix(specifier, "https://") {
		src, err = fetchURL(context.Background(), specifier)
	} else {
		src, err = fs.ReadFile(r.fs, specifier)
	}

	if err != nil {
		r.cache[specifier] = cacheEntry{err: err}
		return nil, err
	}

	mod, err := sobek.ParseModule(specifier, string(src), r.Resolve, parser.WithSourceMapLoader(func(path string) ([]byte, error) {
		return fetchURL(context.Background(), path)
	}))
	if err != nil {
		r.cache[specifier] = cacheEntry{err: err}
		return nil, err
	}

	r.cache[specifier] = cacheEntry{mod: mod}
	return mod, nil
}

func fetchURL(ctx context.Context, u string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, u)
	}

	return io.ReadAll(resp.Body)
}
