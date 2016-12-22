package models

import "github.com/go-openapi/strfmt"




// JSONObject JSON object value.
// swagger:model JsonObject
type JSONObject map[string]JSONValue

// Validate validates this Json object
func (m JSONObject) Validate(formats strfmt.Registry) error {
	return nil
}
