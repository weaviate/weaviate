package kinds

import "fmt"

// ErrInvalidUserInput indicates a client-side error
type ErrInvalidUserInput error

func newErrInvalidUserInput(format string, args ...interface{}) ErrInvalidUserInput {
	return ErrInvalidUserInput(fmt.Errorf(format, args...))
}

// ErrInternal indicates something went wrong during processing
type ErrInternal error

func newErrInternal(format string, args ...interface{}) ErrInternal {
	return ErrInternal(fmt.Errorf(format, args...))
}

// ErrNotFound indicates the desired resource doesn't exist
type ErrNotFound error

func newErrNotFound(format string, args ...interface{}) ErrNotFound {
	return ErrNotFound(fmt.Errorf(format, args...))
}
