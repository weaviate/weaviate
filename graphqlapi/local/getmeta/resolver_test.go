package getmeta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
)

func Test_Resolve(t *testing.T) {
	t.Run("resolve an int prop", func(t *testing.T) {
		t.Parallel()

		resolver := newMockResolver()

		expectedParams := &Params{
			Kind:       kind.THING_KIND,
			ClassName:  "Car",
			Properties: []MetaProperty{{Name: "horsepower", StatisticalAnalyses: []StatisticalAnalysis{Average}}},
		}

		resolver.On("LocalGetMeta", expectedParams).
			Return(map[string]interface{}{}, nil).Once()

		query := "{ GetMeta { Things { Car { horsepower { average } } } } }"
		resolver.AssertResolve(t, query)
	})
}
