//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package storobj

import "fmt"

type ErrNotFound struct {
	DocID       int64
	OriginalMsg string
}

func NewErrNotFoundf(docID int64, msg string, args ...interface{}) error {
	return ErrNotFound{
		DocID:       docID,
		OriginalMsg: fmt.Sprintf(msg, args...),
	}
}

func (err ErrNotFound) Error() string {
	return fmt.Sprintf("no object found for doc id %d: %s", err.DocID, err.OriginalMsg)
}
