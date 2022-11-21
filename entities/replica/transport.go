package replica

const (
	// RequestKey is used to marshalling request IDs
	RequestKey = "request_id"
)

type SimpleResponse struct {
	Errors []string `json:"errors"`
}

func (r *SimpleResponse) FirstError() error {
	for _, msg := range r.Errors {
		if msg != "" {
			return &Error{Msg: msg}
		}
	}
	return nil
}

// DeleteBatchResponse represents the response returned by DeleteObjects
type DeleteBatchResponse struct {
	Batch []UUID2Error `json:"batch,omitempty"`
}

// FirstError returns the first found error
func (r *DeleteBatchResponse) FirstError() error {
	for _, r := range r.Batch {
		if r.Error != "" {
			return &Error{Msg: r.Error}
		}
	}
	return nil
}
