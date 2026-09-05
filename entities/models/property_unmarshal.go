package models

import (
	"encoding/json"
	"sync"

	"github.com/go-openapi/swag"
)

var propertiesWithExplicitEmptyTokenization sync.Map

// UnmarshalJSON keeps track of tokenizable properties that explicitly set
// tokenization to an empty value. The generated Property model uses a plain
// string, so after JSON decoding an omitted tokenization and `"tokenization": ""`
// otherwise look the same to schema defaulting.
func (m *Property) UnmarshalJSON(raw []byte) error {
	ClearExplicitEmptyTokenization(m)

	type propertyWithoutUnmarshal Property

	var property propertyWithoutUnmarshal
	if err := swag.ReadJSON(raw, &property); err != nil {
		return err
	}
	*m = Property(property)

	explicitEmpty, err := hasExplicitEmptyTokenization(raw)
	if err != nil {
		return err
	}
	if explicitEmpty && isTokenizableDataType(m.DataType) {
		propertiesWithExplicitEmptyTokenization.Store(m, struct{}{})
	}

	return nil
}

func hasExplicitEmptyTokenization(raw []byte) (bool, error) {
	var fields struct {
		Tokenization json.RawMessage `json:"tokenization"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false, err
	}
	if fields.Tokenization == nil {
		return false, nil
	}

	var tokenization string
	if err := json.Unmarshal(fields.Tokenization, &tokenization); err != nil {
		return false, err
	}

	return tokenization == "", nil
}

func isTokenizableDataType(dataType []string) bool {
	if len(dataType) != 1 {
		return false
	}

	switch dataType[0] {
	case "string", "string[]", "text", "text[]":
		return true
	default:
		return false
	}
}

// HasExplicitEmptyTokenization reports whether a property was decoded from JSON
// with an explicitly empty tokenization value for a tokenizable data type.
func HasExplicitEmptyTokenization(prop *Property) bool {
	if prop == nil {
		return false
	}
	_, ok := propertiesWithExplicitEmptyTokenization.Load(prop)
	return ok
}

// ConsumeExplicitEmptyTokenization reports and clears whether a property was
// decoded from JSON with an explicitly empty tokenization value.
func ConsumeExplicitEmptyTokenization(prop *Property) bool {
	if prop == nil {
		return false
	}
	_, ok := propertiesWithExplicitEmptyTokenization.LoadAndDelete(prop)
	return ok
}

// ClearExplicitEmptyTokenization clears any explicit-empty marker for a single
// property.
func ClearExplicitEmptyTokenization(prop *Property) {
	if prop == nil {
		return
	}
	propertiesWithExplicitEmptyTokenization.Delete(prop)
}

// ClearExplicitEmptyTokenizations clears any explicit-empty markers for a set
// of properties.
func ClearExplicitEmptyTokenizations(props ...*Property) {
	for _, prop := range props {
		ClearExplicitEmptyTokenization(prop)
	}
}
