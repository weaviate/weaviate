package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertyUnmarshalTracksExplicitEmptyTokenization(t *testing.T) {
	var prop Property
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "title",
		"dataType": ["text"],
		"tokenization": ""
	}`), &prop))

	require.True(t, HasExplicitEmptyTokenization(&prop))
	require.True(t, ConsumeExplicitEmptyTokenization(&prop))
	require.False(t, HasExplicitEmptyTokenization(&prop))
}

func TestPropertyUnmarshalDoesNotTrackOmittedTokenization(t *testing.T) {
	var prop Property
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "title",
		"dataType": ["text"]
	}`), &prop))

	require.False(t, HasExplicitEmptyTokenization(&prop))
}

func TestPropertyUnmarshalClearsPreviousExplicitEmptyTokenization(t *testing.T) {
	var prop Property
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "title",
		"dataType": ["text"],
		"tokenization": ""
	}`), &prop))
	require.True(t, HasExplicitEmptyTokenization(&prop))

	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "title",
		"dataType": ["text"]
	}`), &prop))
	require.False(t, HasExplicitEmptyTokenization(&prop))
}

func TestPropertyUnmarshalDoesNotTrackExplicitEmptyTokenizationForNonTokenizableDataTypes(t *testing.T) {
	for _, raw := range []string{
		`{"name": "count", "dataType": ["int"], "tokenization": ""}`,
		`{"name": "author", "dataType": ["Author"], "tokenization": ""}`,
		`{"name": "metadata", "dataType": ["object"], "tokenization": ""}`,
	} {
		var prop Property
		require.NoError(t, json.Unmarshal([]byte(raw), &prop))
		require.False(t, HasExplicitEmptyTokenization(&prop))
	}
}
