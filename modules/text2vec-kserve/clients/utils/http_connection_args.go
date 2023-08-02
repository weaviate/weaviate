package utils

type HttpConnectionArgs struct{}

func ToHttpConnectionArgs(args map[string]interface{}) (*HttpConnectionArgs, error) {
	return &HttpConnectionArgs{}, nil
}
