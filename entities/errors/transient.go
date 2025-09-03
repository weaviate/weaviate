package errors

import (
	"errors"
	"fmt"
)

func IsTransient(err error) bool {
	if errors.Is(err, NotEnoughMemory) {
		return true
	}

	return false
}

var (
	NotEnoughMemory   = fmt.Errorf("not enough memory")
	NotEnoughMappings = fmt.Errorf("not enough memory mappings")
)

func NewNotEnoughMemory(msg string) error {
	return fmt.Errorf("%s: %w", msg, NotEnoughMemory)
}
