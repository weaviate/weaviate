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
