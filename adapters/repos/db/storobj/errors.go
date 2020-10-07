package storobj

import "fmt"

type ErrNotFound struct {
	DocID       int32
	OriginalMsg string
}

func NewErrNotFoundf(docID int32, msg string, args ...interface{}) error {
	return ErrNotFound{
		DocID:       docID,
		OriginalMsg: fmt.Sprintf(msg, args...),
	}
}

func (err ErrNotFound) Error() string {
	return fmt.Sprintf("no object found for doc id %d: %s", err.DocID, err.OriginalMsg)
}
