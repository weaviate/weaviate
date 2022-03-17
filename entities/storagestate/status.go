package storagestate

import "errors"

const (
	StatusReadOnly Status = "READONLY"
	StatusReady    Status = "READY"
)

var (
	ErrStatusReadOnly = errors.New("store is read-only")
	ErrInvalidStatus  = errors.New("invalid storage status")
)

type Status string

func (s Status) String() string {
	return string(s)
}

func ValidateStatus(in string) (status Status, err error) {
	switch in {
	case string(StatusReadOnly):
		status = StatusReadOnly
	case string(StatusReady):
		status = StatusReady
	default:
		err = ErrInvalidStatus
	}

	return
}
