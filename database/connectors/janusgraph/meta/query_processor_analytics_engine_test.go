package meta

import (
	"context"
	"encoding/json"
	"testing"

	analytics "github.com/SeMI-network/janus-spark-analytics/clients/go"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_QueryProcessor_AnalyticsEngine(t *testing.T) {
	t.Run("when params specify not to use the analytics engine", func(t *testing.T) {
		params := paramsWithAnalyticsProps(cf.AnalyticsProps{})
		executorResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"horsepower": map[string]interface{}{
							"mean": 200,
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: executorResponse}
		expectedResult := map[string]interface{}{
			"horsepower": map[string]interface{}{
				"mean": 200,
			},
		}

		etcd := &etcdClientMock{}
		analytics := &analyticsAPIMock{}

		result, err := NewProcessor(executor, etcd, analytics).
			Process(gremlin.New(), nil, &params)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be directly computed without the engine")

		// Make sure that neither etcd, nor the analytics API were involved
		etcd.AssertNotCalled(t, "Get")
		analytics.AssertNotCalled(t, "Schedule")
	})

	t.Run("when analytics engine should be used and cache is present", func(t *testing.T) {
		params := paramsWithAnalyticsProps(cf.AnalyticsProps{UseAnaltyicsEngine: true})
		executorResponse := &gremlin.Response{Data: []gremlin.Datum{}}
		executor := &fakeExecutor{result: executorResponse}
		expectedResult := map[string]interface{}{
			"horsepower": map[string]interface{}{
				"mean": float64(300),
			},
		}
		hash, err := params.AnalyticsHash()
		require.Nil(t, err)

		etcd := &etcdClientMock{}
		analyticsResult := []interface{}{
			map[string]interface{}{
				"horsepower": map[string]interface{}{
					"mean": 300,
				},
			},
		}
		etcd.On("Get", mock.Anything, keyFromHash(hash),
			[]clientv3.OpOption(nil)).
			Return(etcdResponse(analyticsResult, keyFromHash(hash), analytics.StatusSucceeded), nil)

		analytics := &analyticsAPIMock{}

		result, err := NewProcessor(executor, etcd, analytics).
			Process(gremlin.New(), nil, &params)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be directly computed without the engine")

		// Make sure the analytics API wasn't called (since we retrieved the result from the cache)
		etcd.AssertExpectations(t)
		analytics.AssertNotCalled(t, "Schedule")
	})
}

func paramsWithAnalyticsProps(a cf.AnalyticsProps) getmeta.Params {
	return getmeta.Params{
		Kind:      kind.THING_KIND,
		ClassName: schema.ClassName("Car"),
		Properties: []getmeta.MetaProperty{
			{
				Name:                "horsepower",
				StatisticalAnalyses: []getmeta.StatisticalAnalysis{getmeta.Mean},
			},
		},
		Analytics: a,
	}
}

type etcdClientMock struct {
	mock.Mock
}

func (m *etcdClientMock) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

type analyticsAPIMock struct {
	mock.Mock
}

func (m *analyticsAPIMock) Get(ctx context.Context, params analytics.QueryParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func etcdResponse(data []interface{}, id string, status analytics.Status) *clientv3.GetResponse {
	res := analytics.Result{
		ID:            id,
		Status:        status,
		Result:        data,
		OriginalQuery: analytics.QueryParams{}, // doesn't matter
	}

	resBytes, _ := json.Marshal(res)

	return &clientv3.GetResponse{
		Count: 1,
		Kvs: []*mvccpb.KeyValue{
			&mvccpb.KeyValue{
				Value: resBytes,
			},
		},
	}
}
