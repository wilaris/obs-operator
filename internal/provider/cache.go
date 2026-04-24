package provider

import (
	"sync"

	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	"k8s.io/apimachinery/pkg/types"
)

// Cache stores provider clients shared by reconcilers.
type Cache struct {
	mu          sync.RWMutex
	clients     map[types.NamespacedName]cacheEntry
	closeClient func(*obs.ObsClient)
}

type cacheEntry struct {
	key      cacheKey
	resolved *ResolvedClient
}

// NewCache creates an empty provider client cache.
func NewCache() *Cache {
	return &Cache{
		clients:     make(map[types.NamespacedName]cacheEntry),
		closeClient: closeOBSClient,
	}
}

func (c *Cache) get(key cacheKey) (*ResolvedClient, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.clients[key.ProviderConfig]
	if !ok || entry.key != key {
		return nil, false
	}

	return cloneResolvedClient(entry.resolved), true
}

func (c *Cache) set(key cacheKey, client *obs.ObsClient) *ResolvedClient {
	if c == nil {
		return newResolvedClient(key, client)
	}

	resolved := newResolvedClient(key, client)
	var clientsToClose []*obs.ObsClient

	c.mu.Lock()
	if existing, ok := c.clients[key.ProviderConfig]; ok {
		if existing.key == key {
			cached := cloneResolvedClient(existing.resolved)
			cached.FromCache = true
			clientsToClose = append(clientsToClose, client)
			c.mu.Unlock()

			c.closeClients(clientsToClose)
			return cached
		}
		clientsToClose = append(clientsToClose, existing.resolved.OBS)
	}

	c.clients[key.ProviderConfig] = cacheEntry{
		key:      key,
		resolved: resolved,
	}
	c.mu.Unlock()

	c.closeClients(clientsToClose)
	return cloneResolvedClient(resolved)
}

// InvalidateProvider removes all cached clients for a ProviderConfig.
func (c *Cache) InvalidateProvider(namespace, name string) {
	if c == nil {
		return
	}

	providerKey := types.NamespacedName{Namespace: namespace, Name: name}
	var clientsToClose []*obs.ObsClient

	c.mu.Lock()
	if entry, ok := c.clients[providerKey]; ok {
		clientsToClose = append(clientsToClose, entry.resolved.OBS)
		delete(c.clients, providerKey)
	}
	c.mu.Unlock()

	c.closeClients(clientsToClose)
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

func newResolvedClient(key cacheKey, client *obs.ObsClient) *ResolvedClient {
	return &ResolvedClient{
		OBS:               client,
		ProviderConfig:    key.ProviderConfig,
		CredentialsSecret: key.CredentialsSecret,
		Region:            key.Region,
		Endpoint:          key.Endpoint,
	}
}

func cloneResolvedClient(resolved *ResolvedClient) *ResolvedClient {
	if resolved == nil {
		return nil
	}
	copy := *resolved
	return &copy
}

func (c *Cache) closeClients(clients []*obs.ObsClient) {
	closeClient := c.closeClient
	if closeClient == nil {
		closeClient = closeOBSClient
	}
	for _, client := range clients {
		if client != nil {
			closeClient(client)
		}
	}
}

func closeOBSClient(client *obs.ObsClient) {
	client.Close()
}
