//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backups

type ErrUnprocessable struct {
	err error
}

type ErrNotFound struct {
	err error
}

func NewErrUnprocessable(err error) ErrUnprocessable {
	return ErrUnprocessable{err}
}

func NewErrNotFound(err error) ErrNotFound {
	return ErrNotFound{err}
}

func (e ErrUnprocessable) Error() string {
	return e.err.Error()
}

func (e ErrNotFound) Error() string {
	return e.err.Error()
}
