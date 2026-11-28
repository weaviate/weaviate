//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A payload without a header is refused at add, so these cases decide which tasks the conflict check ever sees.
func TestReindexDecodeHeader(t *testing.T) {
	for _, tc := range []struct {
		name, payload, want string
	}{
		{name: "well-formed payload names its collection", payload: `{"collection":"ProductCatalog","migrationType":"change-tokenization"}`, want: "ProductCatalog"},
		{name: "unknown fields are skipped", payload: `{"collection":"Foo","migrationType":"change-tokenization","futureField":42}`, want: "Foo"},
		{name: "missing collection is refused", payload: `{"migrationType":"enable-rangeable"}`},
		{name: "missing migration type is refused", payload: `{"collection":"Foo"}`},
		{name: "unparseable payload is refused", payload: `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header, err := ReindexDetector{}.DecodeHeader([]byte(tc.payload))
			if tc.want == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, header.Collection())
		})
	}
}

// Adding a payload field must force a decision on whether GET /v1/tasks may show
// it to a caller without tenant read.
func TestTaskPayloadFieldsAreClassifiedForTasksList(t *testing.T) {
	tests := []struct {
		payload any
		visible []string
		hidden  []string
	}{
		{
			payload: ReindexTaskPayload{},
			visible: ReindexTaskVisiblePayloadFields,
			hidden:  []string{"properties", "tenants"},
		},
		{
			payload: DropVectorIndexTaskPayload{},
			visible: DropVectorIndexTaskVisiblePayloadFields,
			hidden:  []string{"targets", "unitToNode", "unitToShard", "cleanedShards", "deferredShards"},
		},
	}
	for _, tt := range tests {
		payloadType := reflect.TypeOf(tt.payload)
		t.Run(payloadType.Name(), func(t *testing.T) {
			fields := make([]string, 0, payloadType.NumField())
			for i := 0; i < payloadType.NumField(); i++ {
				name, _, _ := strings.Cut(payloadType.Field(i).Tag.Get("json"), ",")
				require.NotEmpty(t, name, "field %s needs a json name", payloadType.Field(i).Name)
				fields = append(fields, name)
			}

			require.ElementsMatch(t, fields, append(append([]string{}, tt.visible...), tt.hidden...),
				"each payload field must be in exactly one of the visible or hidden lists")
		})
	}
}
