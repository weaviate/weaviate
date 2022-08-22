package visited

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	// pool with two liss
	pool := NewPool(2, 2)
	assert.Equal(t, 2, len(pool.listSets))

	// get first list
	l1 := pool.Borrow()
	assert.Equal(t, 2, l1.Len())
	assert.Equal(t, 1, len(pool.listSets))

	// get second list
	l2 := pool.Borrow()
	assert.Equal(t, 0, len(pool.listSets))

	// get third list from empty pool and return it back
	l3 := pool.Borrow()
	assert.Equal(t, 0, len(pool.listSets))
	pool.Return(l3)
	assert.Equal(t, 1, len(pool.listSets))

	// get same list again and modify its size
	// so that it is not accepted when returned to the pool
	l3 = pool.Borrow()
	l3.Visit(2)
	pool.Return(l3)
	assert.Equal(t, 0, len(pool.listSets))

	// add two list and destroy the pool
	pool.Return(l1)
	pool.Return(l2)
	assert.Equal(t, 2, len(pool.listSets))
	pool.Destroy()
	assert.Equal(t, 0, len(pool.listSets))
}
