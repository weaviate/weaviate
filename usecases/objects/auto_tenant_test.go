package objects

import (
	"testing"
)

func TestTenantCache(t *testing.T) {
	cache := newTenantCache(3)

	cache.put("a", 1)
	cache.put("b", 2)
	cache.put("c", 3)

	if cache.get("a") != 1 {
		t.Errorf("Expected value 1 for key 'a', but got %v", cache.get("a"))
	}

	cache.put("d", 4)

	if cache.get("b") != nil {
		t.Errorf("Expected value nil for key 'b' after eviction, but got %v", cache.get("b"))
	}

	cache.put("e", 5)

	if cache.len() != 3 {
		t.Errorf("Expected cache length 3, but got %v", cache.len())
	}
}

func TestTenantCache_Overwrite(t *testing.T) {
	cache := newTenantCache(2)

	cache.put("a", 1)
	cache.put("b", 2)
	cache.put("a", 3)

	if cache.get("a") != 3 {
		t.Errorf("Expected value 3 for key 'a', but got %v", cache.get("a"))
	}
}

func TestTenantCache_Capacity(t *testing.T) {
	cache := newTenantCache(2)

	cache.put("a", 1)
	cache.put("b", 2)
	cache.put("c", 3)

	if cache.get("a") != nil {
		t.Errorf("Expected value nil for key 'a' after eviction, but got %v", cache.get("a"))
	}
}
