package errors

import (
	"github.com/pkg/errors"
)

var (
	ErrTenantNotActive = errors.New("tenant not active")
	ErrTenantNotFound  = errors.New("tenant not found")
)

func IsTenantNotFound(err error) bool {
	return errors.Is(errors.Unwrap(err), ErrTenantNotFound)
}
