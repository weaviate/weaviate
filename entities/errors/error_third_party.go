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

type ErrThirdParty struct {
	ErrorFromProvider string
	Provider          string
	StatusCode        int
	RequestID         string
}

func NewErrThirdParty(err string, thirdPartyProvider string, statusCode int, requestID string) ErrThirdParty {
	return ErrThirdParty{
		ErrorFromProvider: err,
		Provider:          thirdPartyProvider,
		StatusCode:        statusCode,
		RequestID:         requestID,
	}
}

func (e ErrThirdParty) Error() string {
	return e.ErrorFromProvider
}
