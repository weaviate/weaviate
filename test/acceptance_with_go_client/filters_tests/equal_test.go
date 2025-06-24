package filters_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

func TestEqualNotEqual(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	retailerEqualWalmart := whereRetailer(filters.Equal, "walmart")
	retailerEqualTarget := whereRetailer(filters.Equal, "target")
	// retailerNotEqualWalmart := whereRetailer(filters.NotEqual, "walmart")
	// retailerNotEqualTarget := whereRetailer(filters.NotEqual, "target")
	currencyEqualUsd := whereCurrency(filters.Equal, "USD")
	currencyEqualEur := whereCurrency(filters.Equal, "EUR")
	// currencyNotEqualUsd := whereCurrency(filters.NotEqual, "USD")
	// currencyNotEqualEur := whereCurrency(filters.NotEqual, "EUR")
	// whereRetailerParent73280 := whereRetailerParentId(filters.Equal, 73280)

	wheres := []*filters.WhereBuilder{
		retailerEqualWalmart,
		// retailerNotEqualTarget,
		// retailerNotEqualWalmart,
		retailerEqualTarget,

		currencyEqualUsd,
		currencyEqualEur,
		// currencyNotEqualUsd,
		// currencyNotEqualEur,

		// whereRetailerParent73280,
	}

	var total time.Duration
	repeat := 5000
	queries := repeat * len(wheres)
	tm := time.Now()
	errors := 0
	for i := 1; i <= repeat; i++ {
		for j := range wheres {
			d, err := runQuery(t, client, wheres[j])
			total += d
			if err != nil {
				errors++
			}
		}
		if i%100 == 0 {
			fmt.Printf("repeated %dx, took %s, errors %d\n", i, time.Since(tm), errors)
			tm = time.Now()
			errors = 0
		}
	}

	fmt.Printf("queries: %d; total duration: %s; avg query duration: %s; qps: %f\n\n",
		queries, total, total/time.Duration(queries), float64(queries)*1e9/float64(total))
}

func whereRetailer(operator filters.WhereOperator, value string) *filters.WhereBuilder {
	return filters.Where().
		WithOperator(operator).
		WithValueText(value).
		WithPath([]string{"c_retailer"})
}

func whereCurrency(operator filters.WhereOperator, value string) *filters.WhereBuilder {
	return filters.Where().
		WithOperator(operator).
		WithValueText(value).
		WithPath([]string{"currency"})
}

func whereRetailerParentId(operator filters.WhereOperator, value int64) *filters.WhereBuilder {
	return filters.Where().
		WithOperator(operator).
		WithValueInt(value).
		WithPath([]string{"retailer_parent_id"})
}

func runQuery(t *testing.T, client *wvt.Client, where *filters.WhereBuilder) (time.Duration, error) {
	query := client.GraphQL().Get().
		WithClassName("Products_v2").
		WithWhere(where).
		WithFields(graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}}).
		Build()

	// fmt.Printf("query %s\n", query)

	st := time.Now()
	resp, err := client.GraphQL().Raw().WithQuery(query).Do(context.Background())
	took := time.Since(st)

	// if err != nil {
	// 	clientErr := &fault.WeaviateClientError{}
	// 	if errors.As(err, &clientErr) {
	// 		fmt.Printf("! error [msg: %#v, statuscode: %#v, err: %#v, isunexpected: %#v]\n",
	// 			clientErr.Msg, clientErr.StatusCode, clientErr.DerivedFromError, clientErr.IsUnexpectedStatusCode)
	// 	}
	// }
	_ = resp

	// require.NoError(t, err)
	// require.Empty(t, resp.Errors)

	return took, err
}
