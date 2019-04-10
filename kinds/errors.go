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
