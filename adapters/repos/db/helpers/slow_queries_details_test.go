package helpers

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryDetailsJourney(t *testing.T) {
	ctx := InitSlowQueryDetails(context.Background())

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			AnnotateSlowQueryLog(ctx, fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
		}()
	}

	wg.Wait()

	details := ExtractSlowQueryDetails(ctx)
	require.Len(t, details, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		assert.Equal(t, value, details[key])
	}
}
