package discovery

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/goccy/go-json"
	googleapismodule "github.com/tiny-systems/googleapis-module"
)

const (
	// DiscoveryListURL is the URL to fetch the list of all Google APIs
	DiscoveryListURL = "https://discovery.googleapis.com/discovery/v1/apis"
)

// Client provides access to Google API Discovery documents
type Client struct {
	httpClient *http.Client

	// Cache for discovery list
	discoveryListCache     *googleapismodule.Discovery
	discoveryListCacheTime time.Time
	discoveryListMu        sync.RWMutex

	// Cache for individual API specs
	apiCache   map[string]*googleapismodule.API
	apiCacheMu sync.RWMutex

	// Cache TTL
	cacheTTL time.Duration
}

// NewClient creates a new Discovery client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		apiCache: make(map[string]*googleapismodule.API),
		cacheTTL: 1 * time.Hour, // Cache for 1 hour
	}
}

// ServiceOption represents a service available in the discovery list
type ServiceOption struct {
	ID          string // e.g., "sheets:v4"
	Name        string // e.g., "sheets"
	Version     string // e.g., "v4"
	Title       string // e.g., "Google Sheets API"
	Description string
	Preferred   bool
}

// GetServices returns a list of available Google API services
func (c *Client) GetServices(ctx context.Context) ([]ServiceOption, error) {
	discovery, err := c.getDiscoveryList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get discovery list: %w", err)
	}

	services := make([]ServiceOption, 0, len(discovery.Items))
	for _, item := range discovery.Items {
		services = append(services, ServiceOption{
			ID:          item.ID,
			Name:        item.Name,
			Version:     item.Version,
			Title:       item.Title,
			Description: item.Description,
			Preferred:   item.Preferred,
		})
	}

	// Sort by title for better UX
	sort.Slice(services, func(i, j int) bool {
		return services[i].Title < services[j].Title
	})

	return services, nil
}

// GetPreferredServices returns only the preferred version of each service
func (c *Client) GetPreferredServices(ctx context.Context) ([]ServiceOption, error) {
	services, err := c.GetServices(ctx)
	if err != nil {
		return nil, err
	}

	preferred := make([]ServiceOption, 0)
	for _, svc := range services {
		if svc.Preferred {
			preferred = append(preferred, svc)
		}
	}

	return preferred, nil
}

// GetAPI fetches the full API specification for a given service ID
func (c *Client) GetAPI(ctx context.Context, serviceID string) (*googleapismodule.API, error) {
	// Check cache first
	c.apiCacheMu.RLock()
	if api, ok := c.apiCache[serviceID]; ok {
		c.apiCacheMu.RUnlock()
		return api, nil
	}
	c.apiCacheMu.RUnlock()

	// Get discovery URL for this service
	discoveryURL, err := c.getDiscoveryURL(ctx, serviceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get discovery URL for %s: %w", serviceID, err)
	}

	// Fetch the API spec
	api, err := c.fetchAPI(ctx, discoveryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch API spec for %s: %w", serviceID, err)
	}

	// Cache it
	c.apiCacheMu.Lock()
	c.apiCache[serviceID] = api
	c.apiCacheMu.Unlock()

	return api, nil
}

// GetMethods returns all available methods for a given service
func (c *Client) GetMethods(ctx context.Context, serviceID string) ([]googleapismodule.MethodInfo, error) {
	api, err := c.GetAPI(ctx, serviceID)
	if err != nil {
		return nil, err
	}

	methods := api.GetAllMethods()

	// Sort by full name for consistent ordering
	sort.Slice(methods, func(i, j int) bool {
		return methods[i].FullName < methods[j].FullName
	})

	return methods, nil
}

// getDiscoveryList fetches or returns cached discovery list
func (c *Client) getDiscoveryList(ctx context.Context) (*googleapismodule.Discovery, error) {
	c.discoveryListMu.RLock()
	if c.discoveryListCache != nil && time.Since(c.discoveryListCacheTime) < c.cacheTTL {
		defer c.discoveryListMu.RUnlock()
		return c.discoveryListCache, nil
	}
	c.discoveryListMu.RUnlock()

	// Fetch fresh list
	c.discoveryListMu.Lock()
	defer c.discoveryListMu.Unlock()

	// Double-check after acquiring write lock
	if c.discoveryListCache != nil && time.Since(c.discoveryListCacheTime) < c.cacheTTL {
		return c.discoveryListCache, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, DiscoveryListURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("discovery list request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var discovery googleapismodule.Discovery
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("failed to decode discovery list: %w", err)
	}

	c.discoveryListCache = &discovery
	c.discoveryListCacheTime = time.Now()

	return &discovery, nil
}

// getDiscoveryURL returns the discovery REST URL for a given service ID
func (c *Client) getDiscoveryURL(ctx context.Context, serviceID string) (string, error) {
	discovery, err := c.getDiscoveryList(ctx)
	if err != nil {
		return "", err
	}

	for _, item := range discovery.Items {
		if item.ID == serviceID {
			return item.DiscoveryRestUrl, nil
		}
	}

	return "", fmt.Errorf("service %s not found in discovery list", serviceID)
}

// fetchAPI fetches an API spec from a discovery URL
func (c *Client) fetchAPI(ctx context.Context, url string) (*googleapismodule.API, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API spec request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var api googleapismodule.API
	if err := json.NewDecoder(resp.Body).Decode(&api); err != nil {
		return nil, fmt.Errorf("failed to decode API spec: %w", err)
	}

	return &api, nil
}

// ClearCache clears all cached data
func (c *Client) ClearCache() {
	c.discoveryListMu.Lock()
	c.discoveryListCache = nil
	c.discoveryListMu.Unlock()

	c.apiCacheMu.Lock()
	c.apiCache = make(map[string]*googleapismodule.API)
	c.apiCacheMu.Unlock()
}
