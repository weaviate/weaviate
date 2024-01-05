//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import "fmt"

type ErrOpenHttpRequest struct {
	err error
}

func (e ErrOpenHttpRequest) Error() string {
	return e.err.Error()
}

func NewErrOpenHttpRequest(err error) ErrOpenHttpRequest {
	return ErrOpenHttpRequest{fmt.Errorf("open http request: %w", err)}
}

type ErrSendHttpRequest struct {
	err error
}

func (e ErrSendHttpRequest) Error() string {
	return e.err.Error()
}

func NewErrSendHttpRequest(err error) ErrSendHttpRequest {
	return ErrSendHttpRequest{fmt.Errorf("send http request: %w", err)}
}

type ErrUnexpectedStatusCode struct {
	err error
}

func (e ErrUnexpectedStatusCode) Error() string {
	return e.err.Error()
}

func NewErrUnexpectedStatusCode(statusCode int, body []byte) ErrUnexpectedStatusCode {
	return ErrUnexpectedStatusCode{
		err: fmt.Errorf("unexpected status code %d (%s)", statusCode, body),
	}
}

type ErrUnmarshalBody struct {
	err error
}

func (e ErrUnmarshalBody) Error() string {
	return e.err.Error()
}

func NewErrUnmarshalBody(err error) ErrUnmarshalBody {
	return ErrUnmarshalBody{fmt.Errorf("unmarshal body: %w", err)}
}
