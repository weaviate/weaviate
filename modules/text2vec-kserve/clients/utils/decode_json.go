package utils

import (
	"encoding/json"
	"io"
)

func DecodeJson(r io.Reader, output *interface{}) error {
	err := json.NewDecoder(r).Decode(&output)
	if err != nil {
		return err
	}
	return nil
}
