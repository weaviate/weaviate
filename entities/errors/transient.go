package errors

import (
	"errors"
	"fmt"
)

func IsTransient(err error) bool {
	if errors.Is(err, OutOfMemory) {
		return true
	}

	return false
}

var OutOfMemory = errors.New("not enough memory")

func NewOutOfMemory(msg string) error {
	return fmt.Errorf("%s: %w", msg, OutOfMemory)
}
