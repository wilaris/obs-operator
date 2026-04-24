package provider

import (
	"sync"

	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
)

// Cache stores provider clients shared by reconcilers.
type Cache struct {
	mu      sync.RWMutex
	clients map[cacheKey]*ResolvedClient
}

// NewCache creates an empty provider client cache.
func NewCache() *Cache {
	return &Cache{
		clients: make(map[cacheKey]*ResolvedClient),
	}
}

func (c *Cache) get(key cacheKey) (*ResolvedClient, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	resolved, ok := c.clients[key]
	return resolved, ok
}

func (c *Cache) set(key cacheKey, client *obs.ObsClient) *ResolvedClient {
	if c == nil {
		return &ResolvedClient{
			OBS:               client,
			ProviderConfig:    key.ProviderConfig,
			CredentialsSecret: key.CredentialsSecret,
			Region:            key.Region,
			Endpoint:          key.Endpoint,
		}
	}

	resolved := &ResolvedClient{
		OBS:               client,
		ProviderConfig:    key.ProviderConfig,
		CredentialsSecret: key.CredentialsSecret,
		Region:            key.Region,
		Endpoint:          key.Endpoint,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[key] = resolved
	return resolved
}

// InvalidateProvider removes all cached clients for a ProviderConfig.
func (c *Cache) InvalidateProvider(namespace, name string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.clients {
		if key.ProviderConfig.Namespace == namespace && key.ProviderConfig.Name == name {
			delete(c.clients, key)
		}
	}
}

// Len returns the number of cached provider clients.
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.clients)
}
