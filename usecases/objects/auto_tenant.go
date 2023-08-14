package objects

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
)

func (m *autoSchemaManager) addTenant(ctx context.Context, principal *models.Principal, class, tenant string) error {
	if !m.config.AutoTenantsEnabled || tenant == "" {
		return nil
	}

	if m.recentTenants == nil {
		// TODO: 1024 was chosen as a sane default. Tune as needed
		m.recentTenants = newTenantCache(1024)
	}

	if found := m.recentTenants.get(tenant); found == nil {
		tenants := []*models.Tenant{{Name: tenant}}
		if err := m.schemaManager.AddTenants(ctx, principal, class, tenants); err != nil {
			if !strings.Contains(err.Error(), fmt.Sprintf("tenant %s already exists", tenant)) {
				return err
			}
		}
		m.recentTenants.put(tenant, struct{}{})
	}

	return nil
}

// tenantCache is a LRU-based store to reduce unnecessary network calls.
// autoschema will check this cache before sending a request to create a
// tenant, and when it misses, the tenant is created and the cache updated.
type tenantCache struct {
	capacity  int
	cache     map[string]*list.Element
	evictList *list.List
	mutex     sync.Mutex
}

type entry struct {
	key   string
	value interface{}
}

func newTenantCache(capacity int) *tenantCache {
	return &tenantCache{
		capacity:  capacity,
		cache:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

func (c *tenantCache) get(key string) interface{} {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.cache[key]; exists {
		c.evictList.MoveToFront(elem)
		return elem.Value.(*entry).value
	}
	return nil
}

func (c *tenantCache) put(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.cache[key]; exists {
		c.evictList.MoveToFront(elem)
		elem.Value.(*entry).value = value
		return
	}

	if len(c.cache) >= c.capacity {
		oldest := c.evictList.Back()
		if oldest != nil {
			delete(c.cache, oldest.Value.(*entry).key)
			c.evictList.Remove(oldest)
		}
	}

	elem := c.evictList.PushFront(&entry{key, value})
	c.cache[key] = elem
}

func (c *tenantCache) len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.cache)
}
