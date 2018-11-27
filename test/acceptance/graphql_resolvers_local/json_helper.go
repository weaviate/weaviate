package test

import (
	"encoding/json"
)

func parseJSONSlice(text string) []interface{} {
	var result []interface{}
	err := json.Unmarshal([]byte(text), &result)

	if err != nil {
		panic(err)
	}

	return result
}
