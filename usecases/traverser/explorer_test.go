package traverser

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/stretchr/testify/assert"
)

func Test_Explorer_GetClass(t *testing.T) {
	explorer := NewExplorer(nil)
	t.Run("when a where filter is set", func(t *testing.T) {
		// TODO: gh-911, replace this with actual functionality

		params := &LocalGetParams{
			Explore: &ExploreParams{
				Values: []string{"foo"},
			},
			Filters: &filters.LocalFilter{},
		}

		_, err := explorer.GetClass(context.Background(), params)
		msg := "combining 'explore' and 'where' parameters not possible yet - coming soon!"
		assert.Equal(t, errors.New(msg), err)
	})

}
